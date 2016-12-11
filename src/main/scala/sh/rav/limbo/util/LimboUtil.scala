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

import java.io.File
import java.net.URLClassLoader
import java.util.jar.{Attributes, JarFile}

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

private[limbo] object LimboUtil {
  private val logger = LoggerFactory.getLogger(LimboUtil.getClass)

  def classOf[T: ClassTag]: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  def isLocalDFRunner(options: PipelineOptions): Boolean = {
    val runner = options.getRunner

    require(runner != null, "Pipeline runner not set!")

    runner.isAssignableFrom(classOf[DirectPipelineRunner]) ||
      runner.isAssignableFrom(classOf[InProcessPipelineRunner])
  }

  def configureLocalGCSAccess(conf: SparkConf): SparkConf = {
    require(conf != null, "Configuration should not be null")

    conf.set("spark.hadoop.fs.gs.impl", classOf[GoogleHadoopFileSystem].toString)
    conf.set("spark.hadoop.fs.gs.auth.service.account.enable", "false")
    conf.set("spark.hadoop.fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
    conf.set("spark.hadoop.fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")
    conf.set("spark.hadoop.fs.gs.project.id", "_THIS_VALUE_DOES_NOT_MATTER_")

    conf
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


  def getClassPathResources(classLoader: ClassLoader): List[String] = {
    require(classLoader.isInstanceOf[URLClassLoader],
      "Current ClassLoader is '" + classLoader + "' only URLClassLoaders are supported")

    // exclude jars from JAVA_HOME and files from current directory
    val javaHome = new File(sys.props("java.home")).getCanonicalPath
    val userDir = new File(sys.props("user.dir")).getCanonicalPath

    val classPathJars = classLoader.asInstanceOf[URLClassLoader]
      .getURLs
      .map(url => new File(url.toURI).getCanonicalPath)
      .filter(p => !p.startsWith(javaHome) && p != userDir)
      .toList

    // fetch jars from classpath jar's manifest Class-Path if present
    val manifestJars =  classPathJars
      .filter(_.endsWith(".jar"))
      .map(p => (p, new JarFile(p).getManifest))
      .filter { case (p, manifest) =>
        manifest != null && manifest.getMainAttributes.containsKey(Attributes.Name.CLASS_PATH)}
      .map { case (p, manifest) => (new File(p).getParentFile,
        manifest.getMainAttributes.getValue(Attributes.Name.CLASS_PATH).split(" ")) }
      .flatMap { case (parent, jars) => jars.map(jar =>
        if (jar.startsWith("/")) {
          jar // accept absolute path as is
        } else {
          new File(parent, jar).getCanonicalPath  // relative path
        })
      }

    logger.debug(s"Classpath jars: ${classPathJars.mkString(":")}")
    logger.debug(s"Manifest jars: ${manifestJars.mkString(":")}")

    // no need to care about duplicates here - should be solved by the SDK uploader
    classPathJars ++ manifestJars
  }

  def isTesting: Boolean = {
    sys.env.contains("LIMBO_TESTING") || sys.props.contains("limbo.testing")
  }
}
