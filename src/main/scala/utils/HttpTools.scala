package utils

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.io.{Source => IoSource}
import scala.util.{ Failure, Success }

trait HttpTools {

  implicit val system: ActorSystem = ActorSystem()
  implicit val executor: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()

  def get(url: String): String = IoSource.fromURL(url).mkString

  def getRequest(url: String): HttpRequest =
    HttpRequest(
      method = HttpMethods.GET,
      uri = url
    )

  def getInFuture(url: String): Future[HttpResponse] = Http().singleRequest(getRequest(url))

}

class StreamingHttpsTools[T](urlList: List[String], parseUrls: ((String, HttpResponse)) => List[T]) extends HttpTools {

  //TODO understand and finish this
  def source = Source(urlList)
    .map(url => getInFuture(url).map((url, _)))
    .mapAsync(4)(identity)
    .map(parseUrls)


}

class QueueingHttpsTools(host: String, queueSize: Int = 8) extends HttpTools {

  // This idea came initially from this blog post:
  // http://kazuhiro.github.io/scala/akka/akka-http/akka-streams/2016/01/31/connection-pooling-with-akka-http-and-source-queue.html
  // https://doc.akka.io/docs/akka-http/10.0.9/scala/http/client-side/host-level.html#using-a-host-connection-pool
  val poolClientFlow = Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](host)

  val queue =
    Source.queue[(HttpRequest, Promise[HttpResponse])](queueSize, OverflowStrategy.backpressure)
      .via(poolClientFlow)
      .toMat(Sink.foreach({
        case ((Success(resp), p)) => p.success(resp)
        case ((Failure(e), p))    => p.failure(e)
      }))(Keep.left)
      .run()

  def queueRequest(request: HttpRequest): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued    => responsePromise.future
      case QueueOfferResult.Dropped     => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
    }
  }

  override def getInFuture(url: String): Future[HttpResponse] = {
    queueRequest(HttpRequest(uri = url))
  }

}