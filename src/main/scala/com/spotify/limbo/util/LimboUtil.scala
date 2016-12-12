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

package com.spotify.limbo.util

import java.io.File
import java.net.URLClassLoader
import java.util.jar.{Attributes, JarFile}

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator
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

}
