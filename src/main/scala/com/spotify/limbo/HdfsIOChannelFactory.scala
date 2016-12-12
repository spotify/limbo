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

import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}

import com.google.cloud.dataflow.sdk.util.IOChannelFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * HDFS compatible implementation of [[IOChannelFactory]]
 * @param conf Hadoop configuration
 */
class HdfsIOChannelFactory(conf: Configuration) extends IOChannelFactory {
  private val fs = FileSystem.get(conf)

  // scalastyle:off method.name
  override def `match`(spec: String): java.util.Collection[String] = {
    import scala.collection.JavaConverters._
    fs.globStatus(new Path(spec)).map(_.getPath.toString).toList.asJavaCollection
  }
  // scalastyle:on method.name

  override def getSizeBytes(spec: String): Long = fs.getFileStatus(new Path(spec)).getLen

  override def isReadSeekEfficient(spec: String): Boolean = false

  override def create(spec: String, mimeType: String): WritableByteChannel = {
    val fd = fs.create(new Path(spec))
    Channels.newChannel(fd.getWrappedStream)
  }

  override def open(spec: String): ReadableByteChannel = {
    val fd = fs.open(new Path(spec))
    Channels.newChannel(fd.getWrappedStream)
  }

  override def resolve(path: String, other: String): String = {
    if (new Path(other).isAbsolute) {
      other
    } else {
      new Path(path, other).toString
    }
  }
}
