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

import java.util.Properties

import com.google.cloud.dataflow.sdk.options.{ApplicationNameOptions, PipelineOptionsFactory}
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner
import com.spotify.scio.ScioContext
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
