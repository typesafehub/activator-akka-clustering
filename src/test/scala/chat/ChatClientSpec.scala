package chat

import org.scalatest.{ BeforeAndAfterAll, FlatSpecLike, Matchers }
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ChatClientSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ChatClientSpec"))

  override def afterAll: Unit = Await.ready(system.terminate(), Duration.Inf)

  "A ChatClient" should "publish messages to chatroom topic" in {
    val mediator = DistributedPubSub(system).mediator
    mediator ! Subscribe("chatroom", testActor)
    val chatClient = system.actorOf(ChatClient.props("user1"))
    chatClient ! ChatClient.Publish("hello")
    val msg = expectMsgType[ChatClient.Message]
    msg.from should be("user1")
    msg.text should be("hello")
  }

}