package chat

import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster

object Main {
  def main(args: Array[String]): Unit = {
    val systemName = "ChatApp"
    val system1 = ActorSystem(systemName)
    val joinAddress = Cluster(system1).selfAddress
    Cluster(system1).join(joinAddress)
    system1.actorOf(Props[MemberListener], "memberListener")
    system1.actorOf(Props[RandomUser], "Ben")
    system1.actorOf(Props[RandomUser], "Kathy")

    Thread.sleep(5000)
    val system2 = ActorSystem(systemName)
    Cluster(system2).join(joinAddress)
    system2.actorOf(Props[RandomUser], "Skye")

    Thread.sleep(10000)
    val system3 = ActorSystem(systemName)
    Cluster(system3).join(joinAddress)
    system3.actorOf(Props[RandomUser], "Miguel")
    system3.actorOf(Props[RandomUser], "Tyler")
  }
}
