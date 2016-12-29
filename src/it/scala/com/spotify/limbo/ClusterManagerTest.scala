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

class ClusterManagerTest extends AsyncFlatSpec with Matchers {

  "ClusterManager" should "be able to create and reuse a dataproc cluster" in {
    val zone = "europe-west1-d"
    val zone3 = "europe-west1-c"
    val cluster = ClusterManager.getDataprocCluster(zone = zone)
    val clusterCreated = cluster.map(c =>
      ClusterManager.clusters.contains(c.getProjectId, zone) should be (true)
    )

    // should be able to reuse cluster
    val clusterReused = cluster.flatMap { _ =>
      val cluster2 = ClusterManager.getDataprocCluster(zone = zone)
      cluster2.map(_ =>
        ClusterManager.clusters.size should be(1)
      )
    }

    // different zone = new cluster
    val clusterCreated2nd = clusterReused.flatMap { _ =>
      val cluster3 = ClusterManager.getDataprocCluster(zone = zone3)
      cluster3.map { c =>
        ClusterManager.clusters.contains(c.getProjectId, zone3) should be(true)
        ClusterManager.clusters.size should be(2)
      }
    }

    for {
      _ <- clusterCreated
      _ <- clusterReused
      _ <- clusterCreated2nd
    } yield succeed
  }

}
