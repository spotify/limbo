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

import com.google.cloud.dataflow.sdk.options.{ApplicationNameOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.spotify.scio.ScioContext
import org.apache.spark.SparkContext

trait TestUtils {

  def runWithContexts(fn: (ScioContext, SparkContext) => Unit): Unit = {
    val scio = getScioContextForTest()
    val spark = getSparkContextForTest()
    fn(scio, spark)
  }

  /** Create a new [[ScioContext]] instance for testing. */
  def getScioContextForTest(): ScioContext = {
    val opts = PipelineOptionsFactory
      .fromArgs(Array("--appName=" + "LimboTest"))
      .as(classOf[ApplicationNameOptions])

    opts.setRunner(classOf[InProcessPipelineRunner])
    ScioContext(opts, List[String]())
  }

  /** Create a new [[SparkContext]] instance for testing. */
  def getSparkContextForTest(): SparkContext = {
    SparkContextProvider.createLocalSparkContext(
      "limbo-test",
      Map("spark.driver.allowMultipleContexts" -> true.toString))
  }

}
