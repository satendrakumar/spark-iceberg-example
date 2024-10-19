package com.techmonad.http

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import com.techmonad.ducksb.Reader
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object HTTPService {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "my-system")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val customerRoutes =
      path("api" / "customer") {
        get {
          parameters("id".as[Int], "from".as[String].optional, "to".as[String].optional) { (id, from, to) =>
            complete(Reader.getCustomer(id, from, to))
          }
        }
      }

    val myExceptionHandler = ExceptionHandler {
      case th =>
        extractUri { uri =>
          logger.error(s"Request to $uri could not be handled normally", th)
          complete(HttpResponse(InternalServerError, entity = "Bad Request"))
        }
    }

     val routes = {
      handleExceptions(myExceptionHandler) {
        handleRejections(ApiRejectionHandler.rejectionHandler) {
          customerRoutes
        }
      }
    }

    val futureBinding = Http().newServerAt("0.0.0.0", 8001).bind(routes)

    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        //redisConsumer.startConsuming()
        logger.info(s"Server online at http://localhost:${address.getPort}/")
      case Failure(ex) =>
        logger.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }

}
