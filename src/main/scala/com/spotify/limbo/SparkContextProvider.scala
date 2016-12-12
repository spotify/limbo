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

import java.net.{URI, URL}

import com.google.cloud.dataflow.sdk.util.{IOChannelUtils, PackageUtil}
import com.spotify.limbo.util.LimboUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object SparkContextProvider {

  private val logger = LoggerFactory.getLogger(SparkContextProvider.getClass)

  /**
   * Creates a YARN based [[SparkContext]] based on provided [[Configuration]]. YARN cluster has to
   * already exist. [[Configuration]] URL has to be reachable.
   */
  def createYarnSparkContext(hadoopConfURL: URL,
                             name: String = "limbo",
                             stagingLocation: String = "/limbo-stagingLocation"): SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("yarn")
    sparkConf.setAppName(name)

    val conf = new Configuration(false)
    // fetch Dataproc Hadoop configuration from the master /conf endpoint
    conf.addResource(hadoopConfURL)
    import scala.collection.JavaConverters._
    // inject Hadoop configuration into spark configuration
    sparkConf.setAll(
      conf.iterator().asScala
        .map(e => s"spark.hadoop.${e.getKey}" -> e.getValue)
        .toList)
    logger.debug(s"Spark conf: ${sparkConf.toDebugString}")

    // prepare to stage spark deps/libraries
    val artifacts = LimboUtil.getClassPathResources(Thread.currentThread().getContextClassLoader)
    IOChannelUtils.setIOFactory("hdfs", new HdfsIOChannelFactory(conf))

    // staging location used for spark deps
    val hadoopFS = conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)
    val limboStagingLocation = s"$hadoopFS$stagingLocation"
    logger.info(s"Limbo staging location is $limboStagingLocation")

    // stage spark deps/libraries
    val staged = PackageUtil.stageClasspathElements(artifacts.asJava, limboStagingLocation)
    sparkConf.set("spark.yarn.jars", staged.asScala.map(_.getLocation).mkString(","))
    logger.debug(s"Staged yarn.jars: ${sparkConf.get("spark.yarn.jars")}")

    // finally create a functional spark context
    // this will submit a yarn application, fetch staged jars etc
    new SparkContext(sparkConf)
  }

  /** Creates a local [[SparkContext]] used for testing or local work */
  def createLocalSparkContext(name: String = "limbo",
                              extraSettings: Map[String, String] = Map.empty): SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setAll(extraSettings)
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName(name)
    new SparkContext(sparkConf)
  }

  /**
   * Creates a Dataproc based [[SparkContext]]. If need be it will create a new Dataproc cluster.
   */
  def createDataprocSparkContext(project: String = "scio-playground",
                                 zone: String = "us-central1-a"): Future[SparkContext] = {
    val cluster = ClusterManager.getDataprocCluster(project, zone)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.collection.JavaConverters._
    cluster
      .map(_.getConfig.getMasterConfig.getInstanceNames.asScala.head)
      .map(n => createYarnSparkContext(new URI(s"http://$n:50070/conf").toURL))
  }

}
