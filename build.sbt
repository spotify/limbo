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

val dataprocApiVersion = "v1-rev9-1.22.0"
val gcsConnectorVersion = "1.5.4-hadoop2"
val googleCloudVersion = "0.6.0"
val scalaTestVersion = "3.0.0"
val scioVersion = "0.2.6"
val sparkVersion = "2.0.2"
val slf4jVersion = "1.7.21"

organization  := "sh.rav"
name          := "limbo"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions  ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-hdfs" % scioVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "com.google.cloud.bigdataoss" % "gcs-connector" % gcsConnectorVersion,
  "com.google.apis" % "google-api-services-dataproc" % dataprocApiVersion,
  "com.google.cloud" % "google-cloud" % googleCloudVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "com.spotify" %% "scio-test" % scioVersion % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

// Exclude some of the netty deps - otherwise spark app master failes to communicate with the driver
excludeDependencies ++= Seq(
  "io.netty" % "netty-buffer",
  "io.netty" % "netty-handler",
  "io.netty" % "netty-common",
  "io.netty" % "netty-codec",
  "io.netty" % "netty-resolver",
  "io.netty" % "netty-tcnative-boringssl",
  "io.netty" % "netty-transport"
)

// otherwise has issues with Hadoop FS service discovery
fork := true

packAutoSettings

test in assembly := {}
assemblyMergeStrategy in assembly ~= { old => {
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("libjansi.jnilib") => MergeStrategy.last
    case s if s.endsWith("jansi.dll") => MergeStrategy.rename
    case s if s.endsWith("libjansi.so") => MergeStrategy.rename
    case s if s.endsWith("libsnappyjava.jnilib") => MergeStrategy.last
    case s if s.endsWith("libsnappyjava.so") => MergeStrategy.last
    case s if s.endsWith("snappyjava_snappy.dll") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.rename
    case s if s.endsWith(".xsd") => MergeStrategy.rename
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case s => old(s)
  }
}
