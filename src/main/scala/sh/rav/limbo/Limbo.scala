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

import java.net.{InetAddress, URI}

import com.alibaba.dcm.DnsCacheManipulator
import com.google.cloud.dataflow.sdk.util.{IOChannelUtils, PackageUtil}
import com.spotify.scio.ContextAndArgs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import sh.rav.limbo.util.LimboUtil

import scala.util.{Failure, Random, Success}

object Limbo {

  private val logger = LoggerFactory.getLogger(Limbo.getClass)

  // scalastyle:off method.length
  def createSparkContextWip(): SparkContext = {
    val instance = "rav-test-us-2-m"
    val limboStagingLocation =  s"hdfs://$instance/limbo-stagingLocation"

    val natIp = GcpHelpers.getNatIPofInstance(instance, "scio-playground", "us-east1-b")
    val internalIp = GcpHelpers.getIPofInstance(instance, "scio-playground", "us-east1-b")

    val masterIp = if (InetAddress.getByName(natIp).isReachable(1000)) {
      logger.info(s"Using nat ip: $natIp")
      DnsCacheManipulator.setDnsCache(instance, natIp)
      natIp
    } else {
      logger.info(s"Using internal ip: $internalIp")
      DnsCacheManipulator.setDnsCache(instance, internalIp)
      internalIp
    }/* else {
      throw new Exception(s"Can't reach the master at $instance")
    }  */

    val workers = DataprocClient.describe("rav-test-us-2", "scio-playground")
      .getConfig.getWorkerConfig.getInstanceNames

    import scala.collection.JavaConverters._
    workers.asScala
      .map(w => w -> (GcpHelpers.getNatIPofInstance(w, "scio-playground", "us-east1-b"),
        GcpHelpers.getIPofInstance(w, "scio-playground", "us-east1-b")))
      .foreach { case (i, ips) =>
        if (InetAddress.getByName(ips._1).isReachable(1000)) {
          logger.info(s"Adding ${ips._1} for $i")
          DnsCacheManipulator.setDnsCache(i, ips._1)
        } else {
          logger.info(s"Adding ${ips._2} for $i")
          DnsCacheManipulator.setDnsCache(i, ips._2)
        }
      }

    val sparkConf = new SparkConf()
    sparkConf.setMaster("yarn")
    //sparkConf.set("spark.submit.deployMode", "cluster")
    sparkConf.setAppName("rav-test")

    /* Only if from local */
    //sparkConf.set("spark.driver.port", "8881")
    //sparkConf.set("spark.driver.host", "localhost")
    /* Only if from local */

    val conf = new Configuration(false)
    conf.addResource(new URI(s"http://$masterIp:50070/conf").toURL)
    import scala.collection.JavaConverters._
    sparkConf.setAll(
      conf.iterator().asScala
        .map(e => s"spark.hadoop.${e.getKey}" -> e.getValue)
        .toList)

    val artifacts = LimboUtil.getClassPathResources(Thread.currentThread().getContextClassLoader)
    IOChannelUtils.setIOFactory("hdfs", new HdfsIOChannelFactory(conf))

    // otherwise spark fails
    val badLibs = Seq("netty-buffer",
      "netty-codec",
      "netty-common",
      "netty-handler",
      "netty-resolver",
      "netty-tcnative-boringssl",
      "netty-transport")
    val filteredArtifacts = artifacts.filter(l => ! badLibs.exists(bl => l.contains(bl)))

    val staged = PackageUtil.stageClasspathElements(filteredArtifacts.asJava, limboStagingLocation)
    sparkConf.set("spark.yarn.jars", staged.asScala.map(_.getLocation).mkString(","))

    new SparkContext(sparkConf)
  }
  // scalastyle:on method.length

  def createLocalSparkContextWip(): SparkContext = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("rav-test")
    new SparkContext(sparkConf)
  }

  def createSparkContext(): Unit = {
    val project = "scio-playground"
    val zone = "us-central1-a"

    import scala.concurrent.ExecutionContext.Implicits.global
    val newCluster = DataprocClient.create("rav-test-" + Random.nextInt(),
                                           project,
                                           zone)
    newCluster.metadata.future.onComplete {
      case Success(metadata) => {
        val instance = metadata.get("clusterName")
          .map(_.asInstanceOf[String])
          .map(e => s"$e-m") // create master hostname
          .getOrElse{
            throw new Exception("No cluster name available in metadata")
          }
      }
      case Failure(e) => {
        logger.error(s"Failed to create the cluster due to $e")
      }
    }
  }

  // scalastyle:off method.length
  def main(argv: Array[String]): Unit = {
    // Init - this will be automated:

    val (sc, args) = ContextAndArgs(argv)
    val (sc1, _) = ContextAndArgs(argv)

    val spark = createSparkContextWip()

    try {
      val scol = sc.parallelize(1 to 2)
      val rdd = scol.toRDD(spark).get
      rdd
        .map(_ * 2)
        .toSCollection(sc1)
        .map(_ / 2)
        .saveAsTextFile(args("output"))
      sc1.close()
    } finally {
      // Cleanup
      spark.stop()
    }
  }
  // scalastyle:on method.length
}
