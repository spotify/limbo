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
import com.spotify.scio.testing.{SCollectionMatchers, PipelineTestUtils => ScioTestUtils}
import org.scalatest.{FlatSpec, Matchers}

class LimboImplicitConversionTest
  extends FlatSpec with Matchers with SCollectionMatchers with ScioTestUtils {

  /** Create a new [[ScioContext]] instance for testing. */
  def forTest(): ScioContext = {
    val opts = PipelineOptionsFactory
      .fromArgs(Array("--appName=" + "LimboTest"))
      .as(classOf[ApplicationNameOptions])

    opts.setRunner(classOf[InProcessPipelineRunner])
    ScioContext(opts, List[String]())
  }

  "Conversion" should "support SCollection to RDD trip" in {
    val expected = 1 to 10
    val sc = forTest()
    val spark = SparkContextProvider.createLocalSparkContext()
    sc.parallelize(1 to 10).toRDD(spark).get.collect() should contain theSameElementsAs expected
  }
}
