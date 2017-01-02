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

organization := "com.spotify"
name         := "limbo"
description  := "Library for seamless transition between framework specific data structures"

val dataprocApiVersion = "v1-rev9-1.22.0"
val gcsConnectorVersion = "1.5.4-hadoop2"
val googleCloudVersion = "0.6.0"
val hadoopVersion = "2.7.2"
val scalaTestVersion = "3.0.0"
val scioVersion = "0.2.8"
val sparkVersion = "2.0.2"
val sparkBigQueryVersion = "0.1.2"
val slf4jVersion = "1.7.21"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked")
javacOptions  ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint:unchecked")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-hdfs" % scioVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "com.spotify" %% "spark-bigquery" % sparkBigQueryVersion,
  "com.google.cloud.bigdataoss" % "gcs-connector" % gcsConnectorVersion,
  "com.google.apis" % "google-api-services-dataproc" % dataprocApiVersion,
  "com.google.cloud" % "google-cloud" % googleCloudVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "it,test",
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests"
)

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

dependencyOverrides ++= Set(
  "com.google.inject" % "guice" % "3.0" // otherwise minicluster tests fail
)

// Add compiler plugin for macros:
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

parallelExecution in Test := false
testForkedParallel in Test := false

// Integration tests
configs( config("it") extend(Test) )
Defaults.itSettings

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

// Coverage settings
coverageExcludedPackages := Seq(
  "<empty>",
  "com\\.spotify\\.limbo\\.Transport"
).mkString(";")

// Release settings
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
releasePublishArtifactsAction := PgpKeys.publishSigned.value
publishMavenStyle             := true
publishArtifact in Test       := false
sonatypeProfileName           := "com.spotify"
pomExtra                      := {
  <url>https://github.com/spotify/limbo</url>
  <scm>
    <url>git@github.com/spotify/limbo.git</url>
    <connection>scm:git:git@github.com:spotify/limbo.git</connection>
  </scm>
  <developers>
    <developer>
      <id>ravwojdyla</id>
      <name>Rafal Wojdyla</name>
      <url>https://twitter.com/ravwojdyla</url>
    </developer>
  </developers>
}

credentials ++= (for {
  username <- sys.env.get("SONATYPE_USERNAME")
  password <- sys.env.get("SONATYPE_PASSWORD")
} yield
Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  username,
  password)).toSeq

pgpPassphrase := (for {
  pass <- sys.env.get("GPG_PASSPHRASE")
} yield pass.toArray
