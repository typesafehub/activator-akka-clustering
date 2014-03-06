package chat

import org.scalatest.{ BeforeAndAfterAll, FlatSpecLike, Matchers }
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe

class ChatClientSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ChatClientSpec"))

  override def afterAll: Unit = system.shutdown()

  "A ChatClient" should "publish messages to chatroom topic" in {
    val mediator = DistributedPubSubExtension(system).mediator
    mediator ! Subscribe("chatroom", testActor)
    val chatClient = system.actorOf(ChatClient.props("user1"))
    chatClient ! ChatClient.Publish("hello")
    val msg = expectMsgType[ChatClient.Message]
    msg.from should be("user1")
    msg.text should be("hello")
  }

}