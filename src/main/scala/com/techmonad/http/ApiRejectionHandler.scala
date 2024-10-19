package com.techmonad.http

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes.{BadRequest, MethodNotAllowed, NotFound}
import akka.http.scaladsl.server.Directives.{complete, extractMethod, extractUnmatchedPath}
import akka.http.scaladsl.server._
import org.slf4j.{Logger, LoggerFactory}

object ApiRejectionHandler {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit def rejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case ValidationRejection(msg, cause) =>
          execute(BadRequest, msg, cause)
        case MissingQueryParamRejection(parameterName) =>
          execute(BadRequest, s"MissingQueryParamRejection: $parameterName")
        case MalformedRequestContentRejection(message, cause) =>
          execute(BadRequest, s"MalformedRequestContentRejection : $message ", Option(cause))
      }
      .handleNotFound {
        extractUnmatchedPath { p =>
          execute(NotFound, s"handleNotFound: The path you requested [$p] does not exist.")
        }
      }
      .handleAll[MethodRejection] { methodRejections =>
        extractUnmatchedPath { path =>
          extractMethod { method =>
            execute(MethodNotAllowed, s"This method($method)/path($path) is not supported.")
          }
        }
      }
      .result()


  private def execute(code: StatusCode, res: String, cause: Option[Throwable] = None): Route = {
    logger.error(s"$res: ", cause.getOrElse(null))
    complete(code, s"$res: ")
  }
}