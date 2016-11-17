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

package sh.rav.limbo.util

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.reflect.ClassTag

private[limbo] object LimboUtil {
  def classOf[T: ClassTag]: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def isLocalDFRunner(options: PipelineOptions): Boolean = {
    val runner = options.getRunner

    require(runner != null, "Pipeline runner not set!")

    runner.isAssignableFrom(classOf[DirectPipelineRunner]) ||
      runner.isAssignableFrom(classOf[InProcessPipelineRunner])
  }

  def configureLocalGCSAccess(conf: Configuration): Configuration = {
    require(conf != null, "Configuration should not be null")

    conf.setClass("fs.gs.impl", classOf[GoogleHadoopFileSystem], classOf[FileSystem])
    conf.setBoolean("fs.gs.auth.service.account.enable", false)
    conf.set("fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
    conf.set("fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")
    conf.set("fs.gs.project.id", "_THIS_VALUE_DOES_NOT_MATTER_")

    conf
  }

  def executeWithBackOff[T](request: AbstractGoogleClientRequest[T], errorMsg: String): T = {
    // Reuse util method from BigQuery
    BigQueryTableRowIterator.executeWithBackOff(request, errorMsg)
  }
}
