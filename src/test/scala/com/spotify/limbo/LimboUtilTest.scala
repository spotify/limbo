/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.limbo

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.{BlockingDataflowPipelineRunner,
                                              DataflowPipelineRunner,
                                              DirectPipelineRunner}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.spotify.limbo.util.LimboUtil
import org.scalatest.{FlatSpec, Matchers}

class LimboUtilTest extends FlatSpec with Matchers {

  "isLocalDFRunner" should "identify DirectPipelineRunner as local runner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DirectPipelineRunner])
    LimboUtil.isLocalDFRunner(opts) shouldBe true
  }

  it should "identify InProcessPipelineRunner as local runner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[InProcessPipelineRunner])
    LimboUtil.isLocalDFRunner(opts) shouldBe true
  }

  it should "identify DataflowPipelineRunner as non local runner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[DataflowPipelineRunner])
    LimboUtil.isLocalDFRunner(opts) shouldBe false
  }

  it should "identify BlockingDataflowPipelineRunner as non local runner" in {
    val opts = PipelineOptionsFactory.create()
    opts.setRunner(classOf[BlockingDataflowPipelineRunner])
    LimboUtil.isLocalDFRunner(opts) shouldBe false
  }

}
