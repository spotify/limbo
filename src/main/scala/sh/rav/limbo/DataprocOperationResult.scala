/*
 * Copyright 2016 Rafal Wojdyla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package sh.rav.limbo

import java.io.IOException

import com.google.api.client.util.{BackOff, BackOffUtils, Sleeper}
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.Operation
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object DataprocOperationResult {
  def apply(internal: Operation, dataprocClient: Dataproc): DataprocOperationResult  =
    new DataprocOperationResult(internal, dataprocClient)
}

class DataprocOperationResult private[limbo] (val internal: Operation,
                                              val dataprocClient: Dataproc) {

  private val logger = LoggerFactory.getLogger(classOf[DataprocOperationResult])

  val responses: Promise[Map[String, AnyRef]] = Promise[Map[String, Object]]
  val metadata: Promise[Map[String, AnyRef]] = Promise[Map[String, Object]]

  val finalState: Future[Unit] = Future[Unit] {
    var backoff = GcpHelpers.messageBackoffFactory.backoff()
    val sleeper = Sleeper.DEFAULT

    do {
      val operationState = try {
        dataprocClient
          .projects()
          .regions()
          .operations()
          .get(internal.getName)
          .execute()
      } catch {
        case e: IOException => {
          logger.debug(s"Could not get state of operation ${internal.getName}", e)
          new Operation().setDone(false)
        }
      }

      if (operationState.getDone != null && operationState.getDone == true) {
        if (operationState.getError != null) {
          responses.failure(new Throwable(operationState.getError.toPrettyString))
          metadata.failure(new Throwable(operationState.getError.toPrettyString))
          backoff = BackOff.STOP_BACKOFF
        } else {
          import scala.collection.JavaConverters._
          responses.success(Option(operationState.getResponse)
            .getOrElse(Map.empty.asJava).asScala.toMap)
          metadata.success(Option(operationState.getMetadata)
            .getOrElse(Map.empty.asJava).asScala.toMap)
          backoff = BackOff.STOP_BACKOFF
        }
      } else {
        backoff.reset()
      }
    } while (BackOffUtils.next(sleeper, backoff))
  }
}
