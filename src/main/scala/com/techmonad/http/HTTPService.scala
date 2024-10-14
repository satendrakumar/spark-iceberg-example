package com.techmonad.http

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import com.techmonad.ducksb.Reader
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object HTTPService {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "my-system")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val route =
      path("api" / "customer") {
        get {
          parameters("from".as[String], "to".as[String]) { (from, to) =>
            complete(Reader.getCustomer(from, to))
          }
        }
      }

    val futureBinding = Http().newServerAt("0.0.0.0", 8001).bind(route)

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
