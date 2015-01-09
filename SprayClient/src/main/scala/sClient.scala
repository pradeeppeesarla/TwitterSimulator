import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import java.util.Random
import java.net.InetAddress
import akka.actor.ActorRef
import scala.collection.mutable.Queue
//import scalaj.http.Http
//import scalaj.http.HttpOptions
import akka.actor._
import akka.io.IO
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future
import spray.can.Http
import spray.util._
import spray.http._
import spray.json._
import HttpMethods._


sealed trait TweetMessage
case object StartTweet extends TweetMessage
case object Schedule extends TweetMessage
case object Connect extends TweetMessage
case class HandShake(noOfUsers: Int) extends TweetMessage // write in client
case class Reply(response: String) extends TweetMessage
case class TweetFromUser(tweet: String, userID: String, types: String, hash: String) extends TweetMessage
case object RequestHomePage extends TweetMessage
case object RequestOwnPage extends TweetMessage
case object RequestMentionPage extends TweetMessage
case class RequestHPage(hash: String) extends TweetMessage
case class RequestOPage(hash: String) extends TweetMessage
case class RequestMPage(hash: String) extends TweetMessage
case class GetHomePage(homepageTweets: List[String], homepageSenders: List[String])
case class GetMentionPage(mentionpageTweets: List[String], mentionpageSenders: List[String])
case class GetOwnPage(ownpage: List[String])
case class Answer(tweet: String, ID: String, frequency: String, hash: String)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val userTweetFormat = jsonFormat4(Answer)
  implicit val reqHomePageFormat = jsonFormat1(RequestHPage)
  implicit val reqOwnPageFormat = jsonFormat1(RequestOPage)
  implicit val reqMentionPageFormat = jsonFormat1(RequestMPage)
  implicit val getHomeFormat = jsonFormat2(GetHomePage)
  implicit val getMentionFormat = jsonFormat2(GetMentionPage)
  implicit val getOwnFormat = jsonFormat1(GetOwnPage)
}

import MyJsonProtocol._
object sClient {
  var serverip: String = null;
  var serverport: Int = 0;
  var ports: Int = 10;
  var start: Long = 0;
  var end: Long = 0;
  var sCount: Int = 0;
  var noOfTweets: Int = 1;
  var server: ActorRef = null
  //var serverAd:String = null
  var usersReferences: ArrayList[ActorRef] = null;
  var noOfusers: Int = 0
  var serverAddress: String = null
  var system: akka.actor.ActorSystem = null
  var totalTweets:Int = 0
  def main(args: Array[String]) {
    if (args.length != 3) {
      print("Improper Arguments")
      return ;
    } else {
      noOfusers = Integer.parseInt(args(0));
      usersReferences = new ArrayList[ActorRef](noOfusers); 
      serverip = args(1)
      serverport = Integer.parseInt(args(2))

      val hostname = InetAddress.getLocalHost.getHostName
      val config = ConfigFactory.parseString(
        """akka{
		  		actor{
		  			provider = "akka.remote.RemoteActorRefProvider"
		  		}
		  		remote{
                   enabled-transports = ["akka.remote.netty.tcp"]
		  			netty.tcp{
						hostname = "127.0.0.1"
						port = 0
					}
				}     
    	}""")

      system = ActorSystem("TwitterSystem", ConfigFactory.load(config))

      //server = system.actorFor("akka.tcp://ServerSystem@" + serverip + ":6140/user/ServerActor") // connection to server 
      var connector = system.actorOf(Props(new Connection()), name = "connector")
      connector ! Connect

    }

  }
  class Connection extends Actor {
    def receive = {
      case Connect =>
        //server ! HandShake(noOfusers)
        //case Reply(response) =>
        //server = system.actorFor("akka.tcp://ServerSystem@" + serverip + ":6140/user/"+response)
        var frequency: String = null
        implicit val tsystem = ActorSystem("TSystem")
        for (i <- 0 until noOfusers) {
          if (i < 0.003 * noOfusers)
            frequency = "topCelebs";
          else if (i < 0.033 * noOfusers)
            frequency = "celebs";
          else if (i < 0.133 * noOfusers)
            frequency = "freqTweeters"
          else if (i < 0.333 * noOfusers)
            frequency = "occasnlTweeters"
          else if (i < 0.733 * noOfusers)
            frequency = "onlyTweeters"
          else
            frequency = "inactive"
          // numOfTweets = tweetCount(util.Random.nextInt(20))
          
          var ref = tsystem.actorOf(Props(new TwitterUser(noOfusers, frequency, i.toString)), name = i.toString)
          usersReferences.add(ref);
          //act ! Schedule
        }
        for (i <- 0 until noOfusers) {
          tsystem.actorSelection("/user/" + i.toString) ! Schedule //start of process
          tsystem.actorSelection("/user/" + i.toString) ! RequestHomePage
          tsystem.actorSelection("/user/" + i.toString) ! RequestOwnPage
          tsystem.actorSelection("/user/" + i.toString) ! RequestMentionPage
        }
    }
  }

  class TwitterUser(noOfusers: Int, freq: String, ID: String)(implicit system: ActorSystem) extends Actor {
    import context._
    import scala.concurrent.duration._
    //server = context.actorFor(serverAd)
    val totalUsers: Int = noOfusers;
    val frequency: String = freq
    var count: Int = 0
    var wakeuptime: Int = 0
    var min: Int = 0
    var max: Int = 0
    var requestHCount:Int = 0;
    var requestOCount:Int = 0;
    var requestMCount:Int = 0;
    
    if (frequency == "topCelebs") {
      min = 1000;
      max = 5000;
    } else if (frequency == "celebs") {
      min = 500;
      max = 2500;
    } else if (frequency == "freqTweeters") {
      min = 200;
      max = 500;
    } else if (frequency == "occasnlTweeters") {
      min = 100;
      max = 150;
    } else if (frequency == "onlyTweeters") {
      min = 20;
      max = 60;
    }

    def receive = {
      case Schedule =>
        if (frequency != "inactive")
          self ! StartTweet
        else{
          totalTweets = totalTweets+1
          implicit val timeout = Timeout(10000 seconds)
          //server ! TweetFromUser("Hello", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString)
		  var jsonTweet = new Answer("HelloHello", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString).toJson
		  var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/ping")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		  var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		  //println(response.entity.asString)
          IO(Http) ? Http.Close

          
          /*val result = Http.postData("http://" + serverip + "/ping", """{"tweet":"HelloHello", "ID":"ID", "frequency":frequency, "hash":usersReferences.get(usersReferences.indexOf(self)).hashCode.toString}""")
        			.header("Content-Type", "application/json")
        			.option(HttpOptions.readTimeout(10000))
        			.option(HttpOptions.connTimeout(10000))
				    .responseCode;*/
        }
      case StartTweet =>
        //var tweet: String = getTweet();
        totalTweets = totalTweets+1
        var tweet: String = "";
        var rand: Int = scala.util.Random.nextInt(100);
        if(rand == 1) {
          //println("Mentioned First");
          var x: String = usersReferences.get(scala.util.Random.nextInt(noOfusers)).hashCode.toString;
          var y: String = usersReferences.get(scala.util.Random.nextInt(noOfusers)).hashCode.toString;
          tweet = "@" + x + " HelloHelloHelloHello" + "@" + y + " HelloHelloHelloHelloHello";
          //din = din + 1;
          //println("X: " + x + " First SendingUser: " + usersReferences.get(usersReferences.indexOf(self)).hashCode.toString);
        }
        else if(rand == 2) {
          //println("Mentioned Middle");
          var x: String = usersReferences.get(scala.util.Random.nextInt(noOfusers)).hashCode.toString;
          tweet = "HelloHelloHelloHelloHello @" + x + " HelloHelloHelloHello";
          //din = din + 1;
          //println("X: " + x + " Second SendingUser: " + usersReferences.get(usersReferences.indexOf(self)).hashCode.toString);
        }
        else {
          tweet = "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello"
        }
        //server ! TweetFromUser(tweet, ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString) //this should be available in server
        if(sCount == 0){
          start = System.currentTimeMillis();
          sCount = sCount + 1;
        }
        
        implicit val timeout = Timeout(10000 seconds)
          //server ! TweetFromUser("Hello", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString)
		  var jsonTweet = new Answer("HelloHello", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString).toJson
		  var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/ping")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		  var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		  //println(response.entity.asString)
          IO(Http) ? Http.Close
        /*val result = Http.postData("http://" + serverip + "/ping", """{"tweet":"HelloHello", "ID":"ID", "frequency":frequency, "hash":usersReferences.get(usersReferences.indexOf(self)).hashCode.toString}""")
        			.header("Content-Type", "application/json")
        			.option(HttpOptions.readTimeout(10000))
        			.option(HttpOptions.connTimeout(10000))
				.responseCode;*/
        count = count + 1;
        if (count < noOfTweets) {
          context.system.scheduler.scheduleOnce(5 milliseconds, self, StartTweet)
        } else if (count >= noOfTweets) {
          count = 0;
          wakeuptime = min + util.Random.nextInt(max - min);
          context.system.scheduler.scheduleOnce(wakeuptime milliseconds, self, Schedule)
        }
        if(totalTweets % 10000 == 0){
          end = System.currentTimeMillis();
          println("Total Time: " + (end - start) + " Total Tweets:" + totalTweets)
        }
        
      case RequestHomePage =>
        if(requestHCount == 0){
          requestHCount= requestHCount+1
          context.system.scheduler.scheduleOnce(5000+util.Random.nextInt(10001) milliseconds, self, RequestHomePage)
        }
        //server ! RequestHPage(self.hashCode.toString)
        implicit val timeout = Timeout(10000 seconds)
		  var jsonTweet = new RequestHPage(self.hashCode.toString).toJson
		  var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/homepage")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		  var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		  var e = response.entity.asString
		  //println(e)
		  if(e == "FAILURE"){
		    //println("Hiii")
		  }
		  else{
		    var m = e.parseJson.convertTo[GetHomePage];
		    self ! GetHomePage(m.homepageTweets, m.homepageSenders);
		  }
          IO(Http) ? Http.Close
          context.system.scheduler.scheduleOnce(20000+util.Random.nextInt(10001) milliseconds, self, RequestHomePage)
        
      case RequestOwnPage =>
        if(requestOCount == 0){
          requestOCount= requestOCount+1
          context.system.scheduler.scheduleOnce(5000+util.Random.nextInt(10001) milliseconds, self, RequestOwnPage)
        }
        //server ! RequestOPage(self.hashCode.toString)
        implicit val timeout = Timeout(10000 seconds)
		  var jsonTweet = new RequestOPage(self.hashCode.toString).toJson
		  var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/ownpage")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		  var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		  //println(response.entity.asString)
        var e = response.entity.asString
		  //println(e)
		  if(e == "FAILURE"){
		    //println("Hiii")
		  }
		  else{
		    var m = e.parseJson.convertTo[GetOwnPage];
		    self ! GetOwnPage(m.ownpage);
		  }
          IO(Http) ? Http.Close
        context.system.scheduler.scheduleOnce(20000+util.Random.nextInt(10001) milliseconds, self, RequestOwnPage)
        
      case RequestMentionPage =>
        if(requestMCount == 0){
          requestMCount= requestMCount + 1
          context.system.scheduler.scheduleOnce(5000+util.Random.nextInt(10001) milliseconds, self, RequestMentionPage)
        }
        //server ! RequestMPage(self.hashCode.toString)
        implicit val timeout = Timeout(10000 seconds)
		  var jsonTweet = new RequestMPage(self.hashCode.toString).toJson
		  var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/mentionpage")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		  var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		  //println(response.entity.asString);
        var e = response.entity.asString
		  //println(e)
		  if(e == "FAILURE"){
		    //println("Hiii")
		  }
		  else{
		    var m = e.parseJson.convertTo[GetMentionPage];
		    self ! GetHomePage(m.mentionpageTweets, m.mentionpageSenders);
		  }
          IO(Http) ? Http.Close
          self ! 
        context.system.scheduler.scheduleOnce(20000+util.Random.nextInt(10001) milliseconds, self, RequestMentionPage)
      
      case GetHomePage(homepageTweets, homepageSenders) =>
        if(homepageTweets != null && homepageTweets.length > 0){
          //println("Home Page for " + this + " is " + homepageSenders.get(0) + " : " + homepageTweets.get(0));
          var randSet: ArrayList[Int] = new ArrayList[Int];
          var limit: Int = 0;
          var i: Int = 0;
          var checked: Int = 0;
          if(frequency == "topCelebs") {
            limit = homepageTweets.length/10;
            checked = 1;
          }
          else if(frequency == "celebs") {
            limit = homepageTweets.length*3/20;
            checked = 1;
          }
          else if(frequency == "freqTweeters") {
            limit = homepageTweets.length/5;
            checked = 1;
          }
          else if(frequency == "occasnlTweeters") {
            limit = homepageTweets.length/5;
            checked = 1;
          }
          else if(frequency == "onlyTweeters") {
          }
          if(checked == 1) {
            var tweet: String = "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello";
            while(i < limit){
              var k: Int = scala.util.Random.nextInt(homepageTweets.length);
              while(randSet != null && randSet.contains(k)) {
                k = scala.util.Random.nextInt(homepageTweets.length);
              }
              randSet.add(k);
              //server ! TweetFromUser(tweet + "(via " + homepageSenders + ")", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString); 
              implicit val timeout = Timeout(10000 seconds)
		      var jsonTweet = new Answer(tweet + "(via " + homepageSenders + ")", ID, frequency, usersReferences.get(usersReferences.indexOf(self)).hashCode.toString).toJson
		      var future = IO(Http).ask(HttpRequest(POST, Uri(s"http://" + serverip + ":" + (serverport + scala.util.Random.nextInt(ports)) +"/ping")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
		      var response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
		      //println(response.entity.asString)
              IO(Http) ? Http.Close
              i = i + 1;
            }
          }
        }
        
      case GetOwnPage(ownpage) =>
        //println("Own Page");
        if(ownpage != null && ownpage.length > 0){
          //println("Own Page for " + this + " is " + ownpage);
        }
        
      case GetMentionPage(mentionpageTweets, mentionpageSenders) =>
        if(mentionpageTweets != null && mentionpageTweets.length > 0) {
          //println("Mention Page for " + this + " is " + mentionpageSenders.get(0) + " : " + mentionpageTweets.get(0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
        }
        
    }

  }

}

class Data(msg: String, sender: ActorRef){
    var tweet: String = msg;
    var tweeter: ActorRef = sender;
    
    /*def Data(msg: String, sender: ActorRef) {
      tweet = msg;
      tweeter = sender;
    }*/
    
    def setTweet(msg: String) {
      tweet = msg;
    }
    
    def setTweeter(sender: ActorRef) {
      tweeter = sender;
    }
    
    def getTweet(): String = {
      return tweet;
    }
    
    def getTweeter(): ActorRef = {
      return tweeter;
    }
  }
