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

package sh.rav.limbo

import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.cloud.dataflow.sdk.values.TypeDescriptor
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import sh.rav.limbo.util.LimboUtil

import scala.reflect.ClassTag
import scala.reflect.io.Path
import scala.util.{Failure, Success}

object Limbo {

  private[limbo] def getNewMaterializePath(sc: ScioContext): String = {
    val filename = "limbo-materialize-" + UUID.randomUUID().toString
    val tmpDir = if (sc.options.getTempLocation == null) {
      sys.props("java.io.tmpdir")
    } else {
      sc.options.getTempLocation
    }
    tmpDir + (if (tmpDir.endsWith("/")) "" else "/") + filename
  }

  implicit class SCollectionToRDD[T: ClassTag](val self: SCollection[T]) {

    private val logger = LoggerFactory.getLogger(self.getClass)

    def toRDD(spark: SparkSession, minPartitions: Int = 0): Option[RDD[T]] = {

      val path = getNewMaterializePath(self.context)

      logger.info(s"Will materialize snapshot of ${self.name} to $path")

      val coder = self.internal
        .getPipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      val hintPartitions = if (minPartitions == 0) {
        spark.sparkContext.defaultMinPartitions
      } else {
        minPartitions
      }

      //TODO: Should we use num of shards to improve min partitions in RDD?
      val snapshot = self
        .map(t => CoderUtils.encodeToBase64(coder, t))
        .saveAsTextFile(path)

      self.context.close()

      snapshot.value match {
        case Some(Success(_)) =>
          val rdd = spark.sparkContext
            .textFile(path, hintPartitions).map(s => CoderUtils.decodeFromBase64(coder, s))
          Some(rdd)
        case Some(Failure(e)) =>
          logger.error(s"Failed to snapshot of SCollection ${self.name} to $path due to $e")
          None
        case _ => {
          logger.error(s"Failed to snapshot ${self.name} to $path")
          None
        }
      }
    }
  }

  implicit class RDDToSCollection[T: ClassTag](val self: RDD[T]) {

    private val logger = LoggerFactory.getLogger(self.getClass)

    def toSCollection(sc: ScioContext): SCollection[T] = {
      val path = getNewMaterializePath(sc)

      val coder = sc.pipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      logger.info(s"Will materialize snapshot of RDD ${self.name} to $path")

      //TODO: should this be some kind of future?
      self.map(t => CoderUtils.encodeToBase64(coder, t)).saveAsTextFile(path)

      // Spark is using Hadoop output format to save as text file, thus we need to use HDFS
      // input method here.
      import com.spotify.scio.hdfs._
      sc.hdfsTextFile(path + (if (path.endsWith("/")) "" else "/") + "*")
        .map(s => CoderUtils.decodeFromBase64(coder, s))
    }
  }

  def main(args: Array[String]): Unit = {
    // Init - this will be automated:
    val (sc, _) = ContextAndArgs(args)
    val spark = SparkSession.builder().master("local").getOrCreate()

    // For tests:
    Path("/tmp/roundtrip").deleteRecursively()

    try {
      val scol = sc.parallelize(1 to 2)
      val rdd = scol.toRDD(spark).get
      val (sc1, _) = ContextAndArgs(args)
      rdd
        .map(_ * 2)
        .toSCollection(sc1)
        .map(_ / 2)
        .saveAsTextFile("/tmp/roundtrip")
      sc1.close()
    } finally {
      // Cleanup
      spark.stop()
    }
  }
}
