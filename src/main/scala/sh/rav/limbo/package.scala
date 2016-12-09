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

package sh.rav

import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.cloud.dataflow.sdk.values.TypeDescriptor
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.values.SCollection
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import sh.rav.limbo.util.LimboUtil

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

package object limbo {

  private[limbo] def getNewMaterializePath(sc: ScioContext): String = {
    val filename = "limbo-materialize-" + UUID.randomUUID().toString
    val tmpDir = if (sc.pipeline.getOptions.getTempLocation == null) {
      sys.props("java.io.tmpdir")
    } else {
      sc.options.getTempLocation
    }
    tmpDir + (if (tmpDir.endsWith("/")) "" else "/") + filename
  }

  implicit class SCollectionToRDD[T: ClassTag](val self: SCollection[T]) {

    private val logger = LoggerFactory.getLogger(self.getClass)

    /** Returns Spark's RDD based on data from this SCollection. */
    def toRDD(spark: SparkContext = null, minPartitions: Int = 0): Option[RDD[T]] = {
      // First materialize/run DF job:
      val path = getNewMaterializePath(self.context)

      logger.info(s"Will materialize SCollection snapshot of ${self.name} to $path")

      val coder = self.internal
        .getPipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      //TODO: Should we use num of shards to improve min partitions in RDD?
      val snapshot = self
        .map(t => CoderUtils.encodeToBase64(coder, t))
        .saveAsTextFile(path)

      import scala.concurrent.ExecutionContext.Implicits.global
      val scioResult = Future(self.context.close())

      // Now figure out where is spark:
      val sparkFuture = if (spark == null) {
        //TODO: Can this be done with Futures all the way down?
        logger.info("Will create a new Spark context. This may take a couple of minutes ...")
        SparkContextProvider.createDataprocSparkContext()
      } else {
        Future.successful(spark)
      }

      // concurrently wait for all futures (failure of any of the 3 will interrupt):
      val wait = for {
        _ <- scioResult
        s <- sparkFuture
        _ <- snapshot
      } yield {
        logger.info("SCollection data materialized, and Spark context is available.")
        s
      }

      // TODO: delete intermediate results?
      // TODO: inf wait - should we just expose future to user facing API?
      val _spark = Await.result(wait, Duration.Inf)

      val hintPartitions = if (minPartitions == 0) {
        _spark.defaultMinPartitions
      } else {
        minPartitions
      }

      Option(_spark
        .textFile(path, hintPartitions).map(s => CoderUtils.decodeFromBase64(coder, s)))
    }
  }

  implicit class RDDToSCollection[T: ClassTag](val self: RDD[T]) {

    private val logger = LoggerFactory.getLogger(self.getClass)

    /** Returns Scio's SCollection based on data from this RDD. */
    def toSCollection(sc: ScioContext): SCollection[T] = {
      val path = getNewMaterializePath(sc)

      val coder = sc.pipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      logger.info(s"Will materialize RDD snapshot of ${self.name} to $path")

      //TODO: should this be some kind of future?
      self.map(t => CoderUtils.encodeToBase64(coder, t)).saveAsTextFile(path)

      // Spark is using Hadoop output format to save as text file, thus we need to use HDFS
      // input method here.
      import com.spotify.scio.hdfs._
      sc.hdfsTextFile(path).map(s => CoderUtils.decodeFromBase64(coder, s))
    }

    /** Returns Scio's SCollection based on data from this RDD. */
    def toSCollection(argv: Array[String]): (ScioContext, SCollection[T]) = {
      val (sc, _) = ContextAndArgs(argv)
      (sc, self.toSCollection(sc))
    }
  }
}
