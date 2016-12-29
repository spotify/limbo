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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.dataproc.DataprocScopes
import com.google.api.services.dataproc.model.{Cluster, ClusterConfig, GceClusterConfig}
import com.google.cloud.dataflow.sdk.options.GcpOptions.DefaultProjectFactory
import com.spotify.limbo.util.LimboUtil
import org.slf4j.LoggerFactory

object DataprocClient {

  private val logger = LoggerFactory.getLogger(DataprocClient.getClass)

  private[limbo] val computeApiRoot = "https://www.googleapis.com/compute/v1"
  private[limbo] lazy val defaultDataprocClient = {
    val username = sys.props("user.name")
    Transport.newDataprocClient(
      GoogleCredential.getApplicationDefault.createScoped(DataprocScopes.all()),
      s"limbo-$username")
  }

  /**
   *  Create Dataproc cluster given the cluster template
   */
  def create(cluster: Cluster, region: String = "global"): DataprocOperationResult = {
    require(cluster.getProjectId != null)
    //TODO: add required fields check?

    val projId = cluster.getProjectId
    val gcpZone = cluster.getConfig.getGceClusterConfig.getZoneUri

    val req = defaultDataprocClient
      .projects()
      .regions()
      .clusters()
      .create(projId, region, cluster)

    DataprocOperationResult(
      LimboUtil.executeWithBackOff(req,
        s"Creation of cluster ${cluster.getClusterName} in project $projId (zone $gcpZone)"),
      defaultDataprocClient)

  }

  /**
   *  Create default Dataproc cluster with a given name
   */
  def create(clusterName: String): DataprocOperationResult = {
    create(clusterName, null, null)
  }

  /**
   *  Create default Dataproc cluster in given project/zone with a given name
   */
  def create(clusterName: String,
             projectId: String,
             zone: String): DataprocOperationResult = {
    //TODO: hard coded xpn project
    //val xpnProject: String = "xpn-master"
    val projId = Option(projectId).getOrElse {
      // gcp java does not expose default project yet
      // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/1380
      new DefaultProjectFactory().create(null)
    }

    val gcpZone = Option(zone).getOrElse{
      val infZone = GcpHelpers.getDefaultZone
      logger.info(s"Inferred default GCP zone $infZone from gcloud. If this is the incorrect "
        + "zone, please specify zone explicitly.")
      infZone
    }

    val region = gcpZone.split("-").take(2).mkString("-")
    val xpnSubNet = "xpn-" + region.split("-").head + region.split("-")(1)(0) + region.last

    val gceClusterConfig = new GceClusterConfig()
      .setZoneUri(s"$computeApiRoot/projects/$projId/zones/$gcpZone")
    //  .setSubnetworkUri(
    //    s"$computeApiRoot/projects/$xpnProject/regions/$region/subnetworks/$xpnSubNet")

    val config = new ClusterConfig()
      .setGceClusterConfig(gceClusterConfig)

    val cluster = new Cluster()
      .setClusterName(clusterName)
      .setProjectId(projId)
      .setConfig(config)

    create(cluster)
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

  def describe(clusterName: String, projectId: String = null, region: String = "global")
  : Cluster = {
    val projId = Option(projectId).getOrElse {
      // gcp java does not expose default project yet
      // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/1380
      new DefaultProjectFactory().create(null)
    }

    val req =
      defaultDataprocClient.projects().regions().clusters().get(projId, region, clusterName)

    LimboUtil.executeWithBackOff(req, s"Get cluster $clusterName in project $projId")
  }

  def list(projectId: String = null, region: String = "global"): Iterable[Cluster] = {
    val projId = Option(projectId).getOrElse {
      // gcp java does not expose default project yet
      // https://github.com/GoogleCloudPlatform/google-cloud-java/pull/1380
      new DefaultProjectFactory().create(null)
    }

    val req = defaultDataprocClient.projects().regions().clusters().list(projId, region)

    val list = LimboUtil.executeWithBackOff(req, s"List clusters in project $projId")
    import scala.collection.JavaConverters._
    Option(list.getClusters).getOrElse(List.empty.asJava).asScala
  }
}
