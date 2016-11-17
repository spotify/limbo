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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.dataproc.model.{Cluster, ClusterConfig, GceClusterConfig}
import com.google.cloud.dataflow.sdk.options.GcpOptions.DefaultProjectFactory
import org.slf4j.LoggerFactory
import sh.rav.limbo.util.LimboUtil

object DataprocClient {

  private val logger = LoggerFactory.getLogger(DataprocClient.getClass)

  private[limbo] val computeApiRoot = "https://www.googleapis.com/compute/v1"
  private[limbo] lazy val defaultDataprocClient = {
    val username = sys.props("user.name")
    Transport.newDataprocClient(GoogleCredential.getApplicationDefault, s"limbo-$username")
  }

  /**
   *  Create Dataproc cluster
   */
  def create(clusterName: String,
             projectId: String = null,
             zone: String = null,
             region: String = "global"): DataprocOperationResult = {
    val projId = Option(projectId).getOrElse {
      // gcp java does not expose default project yet
      // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/1380
      new DefaultProjectFactory().create(null)
    }

    val gcpZone = Option(zone).getOrElse {
      val infZone = GcpHelpers.getDefaultZone
      logger.info(s"Inferred default GCP zone $infZone from gcloud. If this is the incorrect "
        + "zone, please specify zone explicitly.")
      infZone
    }

    val gceClusterConfig = new GceClusterConfig()
      .setZoneUri(s"$computeApiRoot/projects/$projId/zones/$gcpZone")

    val config = new ClusterConfig()
      .setGceClusterConfig(gceClusterConfig)

    val cluster = new Cluster()
      .setClusterName(clusterName)
      .setProjectId(projId)
      .setConfig(config)

    val req = defaultDataprocClient
      .projects()
      .regions()
      .clusters()
      .create(projId, region, cluster)

    DataprocOperationResult(
      LimboUtil.executeWithBackOff(req,
        s"Creation of cluster $clusterName in project $projId (zone $gcpZone)"),
      defaultDataprocClient)
  }

  /**
   * Delete Dataproc cluster
   */
  def delete(clusterName: String, projectId: String = null, region: String = "global")
  : DataprocOperationResult = {
    val projId = Option(projectId).getOrElse {
      // gcp java does not expose default project yet
      // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/1380
      new DefaultProjectFactory().create(null)
    }

    val req =
      defaultDataprocClient.projects().regions().clusters().delete(projId, region, clusterName)

    DataprocOperationResult(
      LimboUtil.executeWithBackOff(req,
        s"Deletion of cluster $clusterName in project $projId"), defaultDataprocClient)

  }
}
