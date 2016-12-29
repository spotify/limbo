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

import org.scalatest.{AsyncFlatSpec, Matchers}

class DataprocClientTest extends AsyncFlatSpec with Matchers {

  "DataprocClient" should "be able to create/describe and delete a Dataproc cluster" in {
    val clusterName = ClusterManager.genClusterName("limbo-it-test")

    val clusterOp = DataprocClient.create(clusterName)
    val createOpDone = clusterOp.finalState.map(_ should be (()))

    // Note: both describe and delete are requested asynchronously, as soon as creation is done

    val listContains = createOpDone.map { _ =>
      val clusters = DataprocClient.list()
      clusters.find(_.getClusterName == clusterName) shouldBe defined
    }

    val nameInDescribeMatch = createOpDone.map { _ =>
      val cluster = DataprocClient.describe(clusterName)
      cluster.getClusterName should be (clusterName)
    }

    val deleteOpDone = createOpDone.flatMap { _ =>
      val deleteOp = DataprocClient.delete(clusterName)
      deleteOp.finalState.map(_ should be (()))
    }

    for {
      _ <- createOpDone
      _ <- listContains
      _ <- nameInDescribeMatch
      _ <- deleteOpDone
    } yield succeed
  }

}
