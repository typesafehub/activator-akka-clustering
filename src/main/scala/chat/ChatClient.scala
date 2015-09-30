package chat

import akka.actor.Actor
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe}

object ChatClient {
  def props(name: String): Props = Props(classOf[ChatClient], name)

  case class Publish(msg: String)
  case class Message(from: String, text: String)
}

class ChatClient(name: String) extends Actor {
  val mediator = DistributedPubSub(context.system).mediator
  val topic = "chatroom"
  mediator ! Subscribe(topic, self)
  println(s"$name joined chat room")

  def receive = {
    case ChatClient.Publish(msg) =>
      mediator ! Publish(topic, ChatClient.Message(name, msg))

    case ChatClient.Message(from, text) =>
      val direction = if (sender == self) ">>>>" else s"<< $from:"
      println(s"$name $direction $text")
  }

}