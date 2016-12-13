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

import java.io.FileWriter
import java.net.URL
import java.nio.file.Files
import java.util.Properties

import com.google.cloud.dataflow.sdk.options.{ApplicationNameOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.spotify.scio.ScioContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapred.MiniMRClientClusterFactory
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkContext

import scala.util.Random

trait TestUtils {

  val limboTestingKey = "limbo.testing"

  def indicateTesting(): Unit = {
    sys.props(limboTestingKey) = true.toString
  }

  def isTesting: Boolean = {
    sys.props.contains(limboTestingKey)
  }

  def runWithLog4jConf(thunk: => Unit, logLevel: String = "WARN"): Unit = {
    val rootLoggerLevel = Logger.getRootLogger.getLevel
    configTestLog4j(logLevel)
    try {
      thunk
    } finally {
      configTestLog4j(rootLoggerLevel.toString)
    }
  }

  def runWithContexts[T](fn: (ScioContext, SparkContext) => T, logLevel: String = "WARN"): T = {
    val rootLoggerLevel = Logger.getRootLogger.getLevel
    configTestLog4j(logLevel)

    val scio = getScioContextForTest()
    val spark = getSparkContextForTest()
    try {
      fn(scio, spark)
    } finally {
      if (!spark.isStopped) spark.stop()
      if (!scio.isClosed) scio.close()
      configTestLog4j(rootLoggerLevel.toString)
    }
  }

  def runWithMiniClusterWithURL(fn: (URL) => Unit, logLevel: String = "ERROR"): Unit = {
    runWithMiniClusterWithConf((conf: Configuration) => {
      val confFile = Files.createTempFile("test-conf", "xml")
      conf.writeXml(new FileWriter(confFile.toString))
      fn(confFile.toUri.toURL)
    }, logLevel)
  }

  def runWithMiniClusterWithConf(fn: (Configuration) => Unit, logLevel: String = "ERROR"): Unit = {
    runWithLog4jConf({
      val dfsBuilder = new MiniDFSCluster.Builder(new Configuration())

      dfsBuilder
        .numDataNodes(1)
        .format(true)
        .manageDataDfsDirs(true)
        .manageNameDfsDirs(true)
      val dfs = dfsBuilder.build()

      val mr = MiniMRClientClusterFactory.create(this.getClass, 1, dfs.getConfiguration(0))

      try {
        fn(mr.getConfig)
      } finally {
        mr.stop()
        if (dfs.isClusterUp) dfs.shutdown()
      }
    }, logLevel)
  }

  /** Create a new [[ScioContext]] instance for testing. */
  def getScioContextForTest(): ScioContext = {
    indicateTesting()
    val opts = PipelineOptionsFactory
      .fromArgs(Array("--appName=" + "LimboTest"))
      .as(classOf[ApplicationNameOptions])

    opts.setRunner(classOf[InProcessPipelineRunner])
    ScioContext(opts, List[String]())
  }

  /** Create a new [[SparkContext]] instance for testing. */
  def getSparkContextForTest(): SparkContext = {
    indicateTesting()
    SparkContextProvider.createLocalSparkContext(
      "limbo-test",
      Map("spark.driver.allowMultipleContexts" -> true.toString,
          "spark.ui.port" -> (Random.nextInt(4040) + 1024).toString))
  }

  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    // suppress the native libs warning
    pro.put("log4j.logger.org.apache.hadoop.util.NativeCodeLoader", "ERROR")
    PropertyConfigurator.configure(pro)
  }

}
