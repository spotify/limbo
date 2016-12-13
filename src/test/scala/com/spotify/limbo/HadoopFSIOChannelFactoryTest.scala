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

import java.net.URI
import java.nio.ByteBuffer

import com.google.cloud.dataflow.sdk.util.MimeTypes
import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class HadoopFSIOChannelFactoryTest extends FlatSpec with Matchers with TestUtils {

  "HadoopFSIOChannelFactory" should "support touch/create" in {
    runWithMiniClusterWithConf { (conf: Configuration) =>
      val dfs = FileSystem.get(conf)
      val testFile = new Path("/foobar")

      dfs.exists(testFile) shouldBe false

      val hdfsIO = new HadoopFSIOChannelFactory(conf)
      val foo = hdfsIO.create(testFile.toUri.getPath, MimeTypes.BINARY)
      foo.close()

      dfs.exists(testFile) shouldBe true
    }
  }

  it should "not support efficient read seek" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.isReadSeekEfficient("foo") shouldBe false
  }

  it should "support match" in {
    runWithMiniClusterWithConf { (conf: Configuration) =>
      val dfs = FileSystem.get(conf)

      dfs.mkdirs(new Path("/test"))
      val toMatch = Seq("/test/foo1", "/test/foo2", "/test/foo3")
      toMatch.foreach { s => dfs.create(new Path(s)).close() }
      dfs.create(new Path("/test/foobar")).close()

      val hdfsIO = new HadoopFSIOChannelFactory(conf)
      val matched = hdfsIO.`match`("/test/foo[1-9]")
      matched should have size 3
      import scala.collection.JavaConverters._
      matched.asScala.map(URI.create).map(_.getPath) should contain theSameElementsAs toMatch
    }
  }

  it should "support write" in {
    runWithMiniClusterWithConf { (conf: Configuration) =>
      val dfs = FileSystem.get(conf)
      val testFile = new Path("/foobar")
      val text = "ala ma kota"
      val bytesToWrite = text.getBytes(Charsets.UTF_8)

      dfs.exists(testFile) shouldBe false

      val hdfsIO = new HadoopFSIOChannelFactory(conf)
      val foo = hdfsIO.create(testFile.toUri.getPath, MimeTypes.BINARY)
      foo.write(ByteBuffer.wrap(bytesToWrite))
      foo.close()

      dfs.exists(testFile) shouldBe true

      val back = ByteBuffer.allocate(bytesToWrite.size).array()
      dfs.open(testFile).readFully(back)

      new String(back, Charsets.UTF_8) shouldBe text
    }
  }

  it should "support open/read" in {
    runWithMiniClusterWithConf { (conf: Configuration) =>
      val dfs = FileSystem.get(conf)
      val testFile = new Path("/foobar")
      val text = "ala ma kota"
      val bytesToWrite = text.getBytes(Charsets.UTF_8)

      dfs.exists(testFile) shouldBe false
      val foo = dfs.create(testFile)
      foo.write(bytesToWrite)
      foo.close()

      val hdfsIO = new HadoopFSIOChannelFactory(conf)
      val foo2 = hdfsIO.open(testFile.toUri.getPath)

      val back = ByteBuffer.allocate(bytesToWrite.size)
      foo2.read(back)
      foo2.close()

      new String(back.array(), Charsets.UTF_8) shouldBe text
    }
  }

  it should "support size" in {
    runWithMiniClusterWithConf { (conf: Configuration) =>
      val dfs = FileSystem.get(conf)
      val testFile = new Path("/foobar")
      val text = "ala ma kota"
      val bytesToWrite = text.getBytes(Charsets.UTF_8)

      dfs.exists(testFile) shouldBe false
      val foo = dfs.create(testFile)
      foo.write(bytesToWrite)
      foo.close()

      val hdfsIO = new HadoopFSIOChannelFactory(conf)
      val size = hdfsIO.getSizeBytes(testFile.toUri.getPath)
      size shouldBe bytesToWrite.size
    }
  }

  it should "support resolve other if other is absolute and path is absolute" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("/bar", "/test/foo") shouldBe "/test/foo"
  }

  it should "support resolve other if other is absolute and path is relative" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("bar", "/test/foo") shouldBe "/test/foo"
  }

  it should "support resolve absolute path if other is empty" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("/bar", "") shouldBe "/bar"
  }

  it should "support resolve relative path if other is empty" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("bar", "") shouldBe "bar"
  }

  it should "support resolve directory if other is relative and path is absolute" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("/bar", "test/foo") shouldBe "/bar/test/foo"
  }

  it should "support resolve directory if other is relative and path is relative" in {
    val hdfsIO = new HadoopFSIOChannelFactory(new Configuration(false))
    hdfsIO.resolve("bar", "test/foo") shouldBe "bar/test/foo"
  }

}
