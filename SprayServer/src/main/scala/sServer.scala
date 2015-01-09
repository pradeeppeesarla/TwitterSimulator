import akka.actor.ActorSystem
import akka.io.IO
import spray.can.Http
import akka.util.Timeout
import scala.concurrent.duration._
import akka.actor.Props
import akka.pattern.ask
import akka.actor.Actor
import spray.routing._
import spray.http._
import spray.http.MediaTypes._
import spray.httpx._
import spray.json._
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import akka.actor.ActorRef
import akka.routing.RoundRobinRouter
import scala.collection.mutable.Queue
import java.util.HashMap
import java.util.Random
import java.util.ArrayList 
import scala.collection.JavaConverters._
  

sealed trait TweetMessage

case class respond(sender: ActorRef) extends TweetMessage
case class TweetInside(tweet: String, userID: String, sender: ActorRef, types: String, hash: String) extends TweetMessage
case class RequestHomeInside(sender:ActorRef, hash: String) extends TweetMessage
case class RequestMentionInside(sender:ActorRef, hash: String) extends TweetMessage
case class RequestOwnInside(sender: ActorRef, hash: String) extends TweetMessage
case class UpdateFollowersList(followersListInt: ArrayList[Int]) extends TweetMessage
case class HomePageTweet(tweet: String, sender: ActorRef)
case class MentionTweetSetup(tweet: String, mentionedUser: ActorRef, sender: ActorRef)
case class MentionTweet(tweet: String, followersOfSender: ArrayList[ActorRef], sender: ActorRef)
case class Mentioned(tweet: String, sender: ActorRef)

case class Answer(tweet: String, ID: String, frequency: String, hash: String)

case class RequestHPage(hash: String) extends TweetMessage
case class RequestOPage(hash: String) extends TweetMessage
case class RequestMPage(hash: String) extends TweetMessage

case class GetHomePage(homepageTweets: List[String], homepageSenders: List[String])
case class GetMentionPage(mentionpageTweets: List[String], mentionpageSenders: List[String])
case class GetOwnPage(ownpage: List[String])

case class Tweet(userId: Int,
                 tweets: List[String],
                 senders: String,
				 timestamp: Long)


object MasterJsonProtocol extends DefaultJsonProtocol {
    implicit val anwserFormat = jsonFormat4(Answer)
    implicit val reqHomePageFormat = jsonFormat1(RequestHPage)
    implicit val reqOwnPageFormat = jsonFormat1(RequestOPage)
    implicit val reqMentionPageFormat = jsonFormat1(RequestMPage)
    implicit val getHomeFormat = jsonFormat2(GetHomePage)
    implicit val getMentionFormat = jsonFormat2(GetMentionPage)
    implicit val getOwnFormat = jsonFormat1(GetOwnPage)
  }

object sServer {

  var prevTweets: Int = 0;
  var totalTweetsCount: Int = 0;
  var actorSystem: akka.actor.ActorSystem = null;
  val tweetListSize: Int = 50;
  var totalUsersRef: ArrayList[String] = new ArrayList[String]();
  var actorMapping: HashMap[String, ActorRef] = new HashMap[String, ActorRef]();
  var topCelebs: Int = 0;
  var celebs: Int = 0;
  var freqTweeters: Int = 0;
  var occasnlTweeters: Int = 0;
  var onlyTweeters: Int = 0;
  var inactive: Int = 0;
  var maxUsers: Int = 0;
  var totalHCount:Int = 0;
  var totalOCount:Int = 0;
  var totalMCount:Int = 0;
  var count: Int = 0;
  var Tier2Router: ActorRef = null;
  
  def main(args: Array[String]) {
  
    maxUsers = Integer.parseInt(args(0))

    //var numOfMaxUsers: Int = maxUsers;
    topCelebs = (0.03 * maxUsers).toInt;
    celebs = (0.003 * maxUsers).toInt;
    freqTweeters = (0.0003 * maxUsers).toInt;
    occasnlTweeters = (0.00003 * maxUsers).toInt;
    onlyTweeters = (0.000003 * maxUsers).toInt;
    inactive = 0;
    
    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("on-spray-can")
    
    actorSystem = ActorSystem("AllActors")

    // create and start our service actor
    val service = system.actorOf(Props[SpraySampleActor], "demo-service")
    val service2 = system.actorOf(Props[SpraySampleActor], "demo-service2")
    val service3 = system.actorOf(Props[SpraySampleActor], "demo-service3")
    val service4 = system.actorOf(Props[SpraySampleActor], "demo-service4")
    val service5 = system.actorOf(Props[SpraySampleActor], "demo-service5")
    val service6 = system.actorOf(Props[SpraySampleActor], "demo-service6")
    val service7 = system.actorOf(Props[SpraySampleActor], "demo-service7")
    val service8 = system.actorOf(Props[SpraySampleActor], "demo-service8")
    val service9 = system.actorOf(Props[SpraySampleActor], "demo-service9")
    val service10 = system.actorOf(Props[SpraySampleActor], "demo-service10")
    
    val h = system.actorOf(Props[TimeHandler], "handler");
    
    h ! "print"

    implicit val timeout = Timeout(5.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = args(1), port = args(2).toInt)
    IO(Http) ? Http.Bind(service2, interface = args(1), port = args(2).toInt + 1)
    IO(Http) ? Http.Bind(service3, interface = args(1), port = args(2).toInt + 2)
    IO(Http) ? Http.Bind(service4, interface = args(1), port = args(2).toInt + 3)
    IO(Http) ? Http.Bind(service5, interface = args(1), port = args(2).toInt + 4)
    IO(Http) ? Http.Bind(service6, interface = args(1), port = args(2).toInt + 5)
    IO(Http) ? Http.Bind(service7, interface = args(1), port = args(2).toInt + 6)
    IO(Http) ? Http.Bind(service8, interface = args(1), port = args(2).toInt + 7)
    IO(Http) ? Http.Bind(service9, interface = args(1), port = args(2).toInt + 8)
    IO(Http) ? Http.Bind(service10, interface = args(1), port = args(2).toInt + 9)
  
  }
  

  

  class TimeHandler extends Actor {
    import context._
    import scala.concurrent.duration._
    def receive = {
      
      case "print" =>
        var prev: Int = prevTweets;
        var now: Int = totalTweetsCount;
        println("TimeFrame : 5s Number of Tweets : " + (now - prev));
        prevTweets = now;
        context.system.scheduler.scheduleOnce(5000 milliseconds, self, "print")
      
    }
    
  }

  // we don't implement our route structure directly in the service actor because
  // we want to be able to test it independently, without having to spin up an actor
  class SpraySampleActor extends Actor {

    var tweets: Int = 0;
    var start: Long = 0;
    var end: Long = 0;
  
    import MasterJsonProtocol._
    import spray.httpx.SprayJsonSupport._
  
    // the HttpService trait defines only one abstract member, which
    // connects the services environment to the enclosing actor or test
    def actorRefFactory = context

    // this actor only runs our route, but you could add
    // other things here, like request stream processing
    // or timeout handling
    /*def receive = {
    //sender ! HttpResponse(entity = "PONG")
    runRoute(myRoute);
    }*/
    def receive = {
      case _: Http.Connected =>
        sender() ! Http.Register(self)
        //println("Hiii");
    
      case HttpRequest(HttpMethods.POST, Uri.Path("/ping"), _, entity, _) =>
        sender ! HttpResponse(entity = "PONG")
        //var system2 = ActorSystem("Hello")
        //var d = system2.actorOf(Props[SampleActor], "han");
        //d ! respond(sender)
        var x = entity.asString.parseJson.convertTo[Answer];
        //println("Byee");
        //runRoute(myRoute);
        //if(count == 0) {
        //  start = System.currentTimeMillis();
        //}
        if (count == 0) {
          Tier2Router = actorSystem.actorOf(Props[Tier2].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count = count + 1;
        }
        Tier2Router ! TweetInside(x.tweet, x.ID, sender, x.frequency, x.hash)
        totalTweetsCount = totalTweetsCount + 1;
        //tweets = tweets + 1;
        /*if(tweets % 1000 == 0) {
          end = System.currentTimeMillis();
          println("Time : " + (end - start) + "Tweets : " + tweets + " Tweet : " + x.tweet + " Hash : " + x.hash);
        }*/
        
      case HttpRequest(HttpMethods.POST, Uri.Path("/homepage"), _, entity, _) =>
        var x = entity.asString.parseJson.convertTo[RequestHPage];
        if (count == 0) {
          Tier2Router = actorSystem.actorOf(Props[Tier2].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count = count + 1;
        }
        Tier2Router ! RequestHomeInside(sender, x.hash)
        
      case HttpRequest(HttpMethods.POST, Uri.Path("/ownpage"), _, entity, _) =>
        var x = entity.asString.parseJson.convertTo[RequestOPage];
        if (count == 0) {
          Tier2Router = actorSystem.actorOf(Props[Tier2].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count = count + 1;
        }
        Tier2Router ! RequestOwnInside(sender, x.hash)
        
      case HttpRequest(HttpMethods.POST, Uri.Path("/mentionpage"), _, entity, _) =>
        var x = entity.asString.parseJson.convertTo[RequestMPage];
        if (count == 0) {
          Tier2Router = actorSystem.actorOf(Props[Tier2].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count = count + 1;
        }
        Tier2Router ! RequestMentionInside(sender, x.hash)
      
      case HttpRequest(HttpMethods.GET, Uri.Path("/ping"), _, _, _) =>
        sender ! HttpResponse(entity = "PONG")
        println("Byee");
        //runRoute(myRoute);
    
    }
  }
  
  import MasterJsonProtocol._
  class HandlerActor extends Actor {
    
    var ownTweetsList: ArrayList[String] = new ArrayList[String];
    var homeTweetsList: ArrayList[String] = new ArrayList[String];
    var homeTweetsListSenders: ArrayList[String] = new ArrayList[String];
    var mentionTweetsList: ArrayList[String] = new ArrayList[String];
    var mentionTweetsListSenders: ArrayList[String] = new ArrayList[String];
    var followersListInArray: ArrayList[Int] = new ArrayList[Int];
    var followers: ArrayList[ActorRef] = new ArrayList[ActorRef];
    var countToCheck: Int = 0;
    var typeOfUser: String = "null";
    
    def receive = {
      
      case TweetInside(tweet, userId, sender, types, hash) =>
        ownTweetsList.add(tweet);
        if(ownTweetsList.size() > tweetListSize) {
          
          ownTweetsList.remove(0);
        }
        typeOfUser = types;
        if(countToCheck != 0 && followersListInArray != null && followersListInArray.size() > followers.size()){
          self ! UpdateFollowersList(followersListInArray);
        }
        var i: Int = 0;
        while(i < followers.size()){
          if(followers.get(i) != null)
            followers.get(i) ! HomePageTweet(tweet, sender);
          i = i + 1;
        }
        
      case RequestHomeInside(sender, hash) =>
        sender ! HttpResponse(entity = HttpEntity(GetHomePage(homeTweetsList.asScala.toList, homeTweetsListSenders.asScala.toList).toJson.toString));
        
      case RequestOwnInside(sender, hash) =>
        sender ! HttpResponse(entity = HttpEntity(GetOwnPage(ownTweetsList.asScala.toList).toJson.toString));
        
      case RequestMentionInside(sender, hash) =>
        sender ! HttpResponse(entity = HttpEntity(GetMentionPage(mentionTweetsList.asScala.toList, mentionTweetsListSenders.asScala.toList).toJson.toString));
        
      case UpdateFollowersList(followersListInt) =>
        if(countToCheck == 0){
          followersListInArray = followersListInt;
          countToCheck = 1;
        }
        var i: Int = 0;
        var j: Int = 0;
        if(followersListInt != null) {
        	while(i < followersListInt.size()){
        		j = followersListInt.get(i);
        		if(j < totalUsersRef.size() && !followers.contains(actorMapping.get(totalUsersRef.get(j)))){
        			followers.add(actorMapping.get(totalUsersRef.get(j)));
        		}
        		i = i + 1;
        	}
        }
        
      case HomePageTweet(tweet, sender) =>
        homeTweetsList.add(tweet);
        homeTweetsListSenders.add(sender.toString);
        if(homeTweetsList.size() > tweetListSize) {
          homeTweetsList.remove(0);
          homeTweetsListSenders.remove(0);
        }
        
      case MentionTweetSetup(tweet, mentionedUser, sender) =>
        mentionedUser ! MentionTweet(tweet, followers, sender)
        ownTweetsList.add(tweet);
        if(ownTweetsList.size() > tweetListSize) {
          ownTweetsList.remove(0);
        }
        
      case MentionTweet(tweet, followersOfSender, sender) =>
        mentionTweetsList.add(tweet);
        mentionTweetsListSenders.add(sender.toString);
        if(mentionTweetsList.size() > tweetListSize) {
          mentionTweetsList.remove(0);
          mentionTweetsListSenders.remove(0);
        }
        if(followersOfSender != null) {
          var iterate: Int = 0;
          if(followersOfSender.contains(self)) {
            homeTweetsList.add(tweet);
            homeTweetsListSenders.add(sender.toString);
            if(homeTweetsList.size() > tweetListSize) {
              homeTweetsList.remove(0);
              homeTweetsListSenders.remove(0);
            }
          }
          while(iterate < followersOfSender.size()) {
            if(followers.contains(followersOfSender.get(iterate))) {
              followersOfSender.get(iterate) ! HomePageTweet(tweet, sender);
            }
            iterate = iterate + 1;
          }
        }
        
      case Mentioned(tweet, sender) =>
        mentionTweetsList.add(tweet);
        mentionTweetsListSenders.add(sender.toString);
        if(mentionTweetsList.size() > tweetListSize) {
          mentionTweetsList.remove(0);
          mentionTweetsListSenders.remove(0);
        }
        
    }
    
  }

  class Tier2 extends Actor {
    var count2: Int = 0
    var Tier3Router: ActorRef = null;
    def receive = {
      case TweetInside(tweet, userId, sender, types, hash) =>
        if (count2 == 0) {
          Tier3Router = actorSystem.actorOf(Props[Tier3].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count2 = count2 + 1;
        }
        Tier3Router ! TweetInside(tweet, userId, sender, types, hash)
        
      case RequestHomeInside(sender, hash) =>
        if (count2 == 0) {
          Tier3Router = actorSystem.actorOf(Props[Tier3].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count2 = count2 + 1;
        }
        Tier3Router ! RequestHomeInside(sender, hash)
        
      case RequestOwnInside(sender, hash) =>
        if (count2 == 0) {
          Tier3Router = actorSystem.actorOf(Props[Tier3].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count2 = count2 + 1;
        }
        Tier3Router ! RequestOwnInside(sender, hash)
        
      case RequestMentionInside(sender, hash) =>
        if (count2 == 0) {
          Tier3Router = actorSystem.actorOf(Props[Tier3].withRouter(RoundRobinRouter(nrOfInstances = 8)))
          count2 = count2 + 1;
        }
        Tier3Router ! RequestMentionInside(sender, hash)
        
    }
  }

  class Tier3 extends Actor {
    
    var actorMapped: ActorRef = null;
    var flag: Int = 0;
    
    def receive = {
      case TweetInside(tweet, userId, sender, types, hash) =>
        
        if (!totalUsersRef.contains(hash)) {
          actorMapped = actorSystem.actorOf(Props(new HandlerActor));
          var count5: Int = 0;
          while(actorMapped == null) {
	       actorMapped = actorSystem.actorOf(Props(new HandlerActor));
            count5 = count5 + 1;
            if(count5 % 100 == 0){
              println(count5)
            }
          }
          actorMapping.put(hash, actorMapped);
          totalUsersRef.add(hash);
          flag = 1;
        }
        else {
          actorMapped = actorMapping.get(hash);
        }
        var splitStrings: Array[String] = null;
        var flag2: Int = 0;
        if(tweet.startsWith("@")) {
          splitStrings = tweet.split(" ");
          if(splitStrings.length > 0) {
            var mention: String = splitStrings(0).substring(1, splitStrings(0).length);
            //println("Mention : " + mention + " SendingUser : " + hash);
            if(actorMapping.containsKey(mention)) {
              //println("Mentioned first");
              actorMapping.get(hash) ! MentionTweetSetup(tweet, actorMapping.get(mention), sender);
              flag2 = 1;
            }
          }
          var splitStrings2: Array[String] = tweet.split("@");
          if(splitStrings2.length > 1) {
            var iterate: Int = 1;
            while(iterate < splitStrings2.length){
              var y: String = splitStrings2(iterate).split(" ")(0)
              //println("Men : " + y + " SendingUser : " + hash);
              if(actorMapping.containsKey(y)) {
                //println("Mentioned middle");
                actorMapping.get(splitStrings2(iterate).split(" ")(0)) ! Mentioned(tweet, sender);
              }
              iterate = iterate + 1;
            }
          }
        }
        else if(tweet.contains("@")) {
          var splitStrings2: Array[String] = tweet.split("@");
          if(splitStrings2.length > 1) {
            var iterate: Int = 1;
            while(iterate < splitStrings2.length){
              var y: String = splitStrings2(iterate).split(" ")(0)
              //println("Men : " + y + " SendingUser : " + hash);
              if(actorMapping.containsKey(y)) {
                //println("Mentioned middle");
                actorMapping.get(splitStrings2(iterate).split(" ")(0)) ! Mentioned(tweet, sender);
              }
              iterate = iterate + 1;
            }
          }

        }
        if(flag2 == 0) {
          if(actorMapped != null)
          actorMapped ! TweetInside(tweet, userId, sender, types, hash);
        }
        
        if(flag == 1){
          flag = 0;
          var num: Int = 0;
          var i: Int = 0;
          var r = new Random();
          if (types.equalsIgnoreCase("topCelebs")) {
            num = topCelebs
          } else if (types.equalsIgnoreCase("celebs")) {
            num = celebs
          } else if (types.equalsIgnoreCase("freqTweeters")) {
            num = freqTweeters
          } else if (types.equalsIgnoreCase("occasnlTweeters")) {
            num = occasnlTweeters
          } else if (types.equalsIgnoreCase("onlyTweeters")) {
            num = onlyTweeters
          } else {
            num = inactive
          }
          var j: Int = 0;
          var al: ArrayList[Int] = new ArrayList[Int];
          while (i < num) {
            j = r.nextInt(maxUsers)
            if (!al.contains(j) && (j >= totalUsersRef.size() || totalUsersRef.get(j) != hash)) {
              al.add(j);
              i = i + 1
            }
          }
          if (num > 0)
            actorMapped ! UpdateFollowersList(al);
          else
            actorMapped ! UpdateFollowersList(null)
        }
        
        
        /* Hello
         
          
          if (!totalUsersRef.contains(sender)) {
          var num: Int = 0;
          var i: Int = 0
          var r = new Random()
          var j: Int = 0
          var al: ArrayList[Int] = new ArrayList[Int];
          var s1: ArrayList[String] = new ArrayList[String];
          s1.add(tweet)
          selfTweetsList.put(sender, s1)
          totalUsersRef.add(sender)
          if (types.equalsIgnoreCase("topCelebs")) {
            num = topCelebs
          } else if (types.equalsIgnoreCase("celebs")) {
            num = celebs
          } else if (types.equalsIgnoreCase("freqTweeters")) {
            num = freqTweeters
          } else if (types.equalsIgnoreCase("occasnlTweeters")) {
            num = occasnlTweeters
          } else if (types.equalsIgnoreCase("onlyTweeters")) {
            num = onlyTweeters
          } else {
            num = inactive
          }
          while (i < num) {
            j = r.nextInt(maxUsers)
            if (!al.contains(j) && (j >= totalUsersRef.size() || totalUsersRef.get(j) != sender)) {
              al.add(j);
              i = i + 1
            }
          }
          if (num > 0)
            followersList.put(sender, al);
          else
            followersList.put(sender, null)
        }else{
          var s2: ArrayList[String] = new ArrayList[String]
          
          s2 = selfTweetsList.get(sender)
          s2.add(tweet)
          if (s2.size() >= tweetListSize) {
            s2.remove(0)
          }
          selfTweetsList.replace(sender, s2)
        
        }
        
        
        Hello*/

        /*
        
         
         
        var followers: ArrayList[Int] = followersList.get(sender);
        var follower: ActorRef = null
        var t1: ArrayList[String] = null
        if (followers != null) {

          for (i <- 0 until followers.size()) {

            t1 = new ArrayList[String];
            var x = followers.get(i)
            if(x<totalUsersRef.size()){
            follower = totalUsersRef.get(x)
            if (tweetsList.containsKey(follower)) {
              t1 = tweetsList.get(follower)
              t1.add(tweet) //adding tweet to followers list
              if (t1.size() >= tweetListSize) {
                t1.remove(0)
              }
              tweetsList.replace(follower, t1)
            } else {
              t1.add(tweet)
              tweetsList.put(follower, t1)
            }
          }
          }
        }
        
        
        
        */


        //totalTweetsCount = totalTweetsCount + 1
        
        //process Tweet Here
        /*if (totalTweetsCount == 0)
          startTime = System.currentTimeMillis();

        totalTweetsCount = totalTweetsCount + 1
        if (totalTweetsCount % 10000 == 0) {
          println("Tweets Processed :" + totalTweetsCount + "  Time taken : " + (System.currentTimeMillis() - startTime))
        }*/
        
        
      case RequestHomeInside(sender, hash) =>
        if(actorMapping.containsKey(hash)){
         // println(tweetsList.get(sender))
          actorMapping.get(hash) ! RequestHomeInside(sender, hash);
        }else{
          //println("Received req for homepage before creation !!!! ERROR");
          
          //sender ! HttpResponse(null, null);
          //sender ! HttpResponse(entity = new Gson().toJson(GetHomePage(null, null)));
          //sender ! HttpResponse(entity = HttpEntity(GetHomePage(null, null).toJson.toString));
          sender ! HttpResponse(entity = "FAILURE");
        }
          
         totalHCount = totalHCount + 1
        if (totalHCount % 1000 == 0) {
          println("HomePages Processed :" + totalHCount )
        }
        
      case RequestOwnInside(sender, hash) =>
        if(actorMapping.containsKey(hash)){
          actorMapping.get(hash) ! RequestOwnInside(sender, hash);
        }
        else {
          //println("Received req for ownpage before creation !!!! ERROR");
          //sender ! GetOwnPage(null);
          //sender ! HttpResponse(entity = new Gson().toJson(GetOwnPage(null)));
          //sender ! HttpResponse(entity = HttpEntity(GetOwnPage(null).toJson.toString));
          sender ! HttpResponse(entity = "FAILURE");
        }

        totalOCount = totalOCount + 1
        if (totalOCount % 1000 == 0) {
          println("OwnPages Processed :" + totalOCount )
        }
          
      case RequestMentionInside(sender, hash) =>
        if(actorMapping.containsKey(hash)){
          // println(tweetsList.get(sender))
          actorMapping.get(hash) ! RequestMentionInside(sender, hash);
        }else{
          //println("Received req for mentionpage before creation !!!! ERROR");
          //sender ! GetMentionPage(null, null);
          //sender ! HttpResponse(entity = new Gson().toJson(GetMentionPage(null, null)));
          //sender ! HttpResponse(entity = HttpEntity(GetMentionPage(null, null).toJson.toString));
          sender ! HttpResponse(entity = "FAILURE");
        }
          
         totalMCount = totalMCount + 1
        if (totalMCount % 1000 == 0) {
          println("MentionPages Processed :" + totalHCount )
        }  
          
    }
  }

  class SampleActor extends Actor {
  
    def receive = {
    
      case respond(sender) =>
        sender ! HttpResponse(entity = "PONG HELLO")
    
    }
  
  }

  
}