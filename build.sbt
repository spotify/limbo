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

val scioVersion = "0.2.6"
val sparkVersion = "2.0.1"

organization  := "sh.rav"
name          := "limbo"

scalaVersion  := "2.11.8"

scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions  ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-hdfs" % scioVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
  )

// otherwise has issues with Hadoop FS service discovery
fork := true
