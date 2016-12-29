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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

import com.google.cloud.compute.{ComputeOptions, Instance, InstanceId}
import com.google.cloud.dataflow.sdk.util.FluentBackoff
import com.google.common.io.Files
import org.joda.time.Duration
import org.slf4j.LoggerFactory


object GcpHelpers {

  private val logger = LoggerFactory.getLogger(GcpHelpers.getClass)

  private def isWindows = {
    sys.props("os.name").toLowerCase(Locale.ENGLISH).contains("windows")
  }

  private[limbo] def getDefaultZone: String = {
    try {
      val configFile = if (sys.env.contains("CLOUDSDK_CONFIG")) {
        new File(sys.env("CLOUDSDK_CONFIG"), "properties")
      } else if (isWindows && sys.env.contains("APPDATA")) {
        new File(sys.env("APPDATA"), "gcloud/properties")
      } else {
        val f = new File(sys.props("user.home"), ".config/gcloud/configurations/config_default")
        if (f.exists()) {
          f
        } else {
          new File(sys.props("user.home"), ".config/gcloud/properties")
        }
      }
      getDefaultZone(configFile)
    } catch {
      case e: Exception => {
        logger.error("Failed to find default zone.")
        throw e
      }
    }
  }

  private[limbo] def getDefaultZone(propertyFile: File) = {
    val zonePattern = "^zone\\s*=\\s*(.*)$".r
    val computeSectionPattern = "^\\[compute\\]$".r
    val sectionPattern = "^\\[(.*)\\]$".r

    import scala.collection.JavaConversions._
    Files.readLines(propertyFile, StandardCharsets.UTF_8).toList
      .map(_.trim)
      .filter(_.nonEmpty)
      .dropWhile(!computeSectionPattern.pattern.matcher(_).matches())
      .drop(1) // drop the compute section header
      .takeWhile(!sectionPattern.pattern.matcher(_).matches())
      .find(zonePattern.pattern.matcher(_).matches())
      .map { s =>
        val matcher = zonePattern.pattern.matcher(s)
        matcher.matches()
        matcher.group(1).trim
      }
      .getOrElse {
        throw new Exception(s"Zone not in the config file $propertyFile")
      }
  }

  private[limbo] def getGcpInstance(instanceName: String, projectId: String, zone: String)
  : Instance = {
    val instance = InstanceId.of(projectId, zone, instanceName)

    val optionsBuilder = ComputeOptions.newBuilder
    val compute = optionsBuilder.build().getService

    compute.getInstance(instance)
  }

  private[limbo] def getNatIPofInstance(instanceName: String, projectId: String, zone: String)
  : String = {
    import scala.collection.JavaConverters._
    GcpHelpers.getGcpInstance(instanceName, projectId, zone).getNetworkInterfaces.asScala
      .flatMap(_.getAccessConfigurations.asScala.map(_.getNatIp))
      .head
  }

  private[limbo] def getIPofInstance(instanceName: String, projectId: String, zone: String)
  : String = {
    import scala.collection.JavaConverters._
    GcpHelpers.getGcpInstance(instanceName, projectId, zone).getNetworkInterfaces.asScala
      .map(_.getNetworkIp)
      .head
  }

  private[limbo] val messageBackoffFactory: FluentBackoff =
    FluentBackoff.DEFAULT
      .withInitialBackoff(Duration.standardSeconds(2))
      .withMaxRetries(11).withExponent(1.5)

}
