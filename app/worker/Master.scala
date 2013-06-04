package worker

import scala.collection.immutable.Queue
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator
import akka.contrib.pattern.DistributedPubSubMediator.Put

object Master {

  val ResultsTopic = "results"

  case class Ack(workId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(work: Work) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus)

}

class Master extends Actor with ActorLogging {
  import Master._
  import MasterWorkerProtocol._
  val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Put(self)

  private var workers = Map[String, WorkerState]()
  private var pendingWork = Queue[Work]()
  private var workIds = Set[String]()

  def receive = {
    case WorkerCreated(workerId) ⇒
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender))
      } else {
        log.debug("Worker registered: {}", workerId)
        workers += (workerId -> WorkerState(sender, status = Idle))
        if (pendingWork.nonEmpty)
          sender ! WorkIsReady
      }

    case WorkerRequestsWork(workerId) ⇒
      if (pendingWork.nonEmpty) {
        workers.get(workerId) match {
          case Some(s @ WorkerState(_, Idle)) ⇒
            val (work, rest) = pendingWork.dequeue
            pendingWork = rest
            log.debug("Giving worker {} some work {}", workerId, work.job)
            sender ! work
            workers += (workerId -> s.copy(status = Busy(work)))
          case _ ⇒

        }
      }

    case WorkIsDone(workerId, workId, result) ⇒
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work))) if work.workId == workId ⇒
          log.debug("Work is done: {} => {} by worker {}", work, result, workerId)
          workers += (workerId -> s.copy(status = Idle))
          mediator ! DistributedPubSubMediator.Publish(ResultsTopic, WorkResult(workId, result))
          sender ! MasterWorkerProtocol.Ack(workId)
        case _ ⇒
          if (workIds.contains(workId)) {
            // previous Ack was lost, confirm again that this is done
            sender ! MasterWorkerProtocol.Ack(workId)
          }
      }

    case WorkFailed(workerId, workId) ⇒
      workers.get(workerId) match {
        case Some(s @ WorkerState(_, Busy(work))) if work.workId == workId ⇒
          log.info("Work failed: {}", work)
          workers += (workerId -> s.copy(status = Idle))
          pendingWork = pendingWork enqueue work
          notifyWorkers()
        case _ ⇒
      }

    case work: Work ⇒
      // idempotent
      if (workIds.contains(work.workId)) {
        sender ! Master.Ack(work.workId)
      } else {
        pendingWork = pendingWork enqueue work
        workIds += work.workId
        log.debug("Accepted work: {}", work)
        sender ! Master.Ack(work.workId)
        notifyWorkers()
      }
  }

  def notifyWorkers(): Unit =
    if (pendingWork.nonEmpty) {
      // could pick a few random instead of all
      workers.foreach {
        case (_, WorkerState(ref, Idle)) ⇒ ref ! WorkIsReady
        case _                           ⇒ // busy
      }
    }

  // TODO fail work without progress
  // TODO cleanup old workers
  // TODO cleanup old workIds

}