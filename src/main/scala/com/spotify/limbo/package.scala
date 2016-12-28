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

package com.spotify

import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.cloud.dataflow.sdk.values.TypeDescriptor
import com.spotify.limbo.util.LimboUtil
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.io.{BigQueryTap, Tap, TextTap}
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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

    /** Returns a [[Future]] of a Spark's [[RDD]] based on the data from this [[SCollection]]. */
    def toRDD(spark: SparkContext): Future[RDD[T]] = {
      self.toRDD(Future.successful(spark))
    }

    /** Returns a [[Future]] of a Spark's [[RDD]] based on the data from this [[SCollection]]. */
    def toRDD(spark: Future[SparkContext] = null): Future[RDD[T]] = {
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

      // Now figure out where is spark:
      val sparkFuture = Option(spark).getOrElse {
        //TODO: Can this be done with Futures all the way down?
        logger.info("Will create a new Spark context. This may take a couple of minutes ...")
        SparkContextProvider.createDataprocSparkContext()
      }

      for {
        s <- sparkFuture
        _ <- snapshot
      } yield {
        logger.info("SCollection data materialized, and Spark context is available.")
        // TODO: hint about partitions?
        // TODO: delete intermediate results?
        s.textFile(path).map(s => CoderUtils.decodeFromBase64(coder, s))
      }
    }
  }

  implicit class RDDToSCollection[T: ClassTag](val self: RDD[T]) {

    private val logger = LoggerFactory.getLogger(self.getClass)

    /** Returns a Scio's [[SCollection]] based on the data from this [[RDD]]. */
    def toSCollection(sc: ScioContext): SCollection[T] = {
      val path = getNewMaterializePath(sc)

      val coder = sc.pipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      logger.info(s"Will materialize RDD snapshot of ${self.toString()} to $path")

      //TODO: should this be some kind of future?
      self.map(t => CoderUtils.encodeToBase64(coder, t)).saveAsTextFile(path)

      // Spark is using Hadoop output format to save as text file, thus we need to use HDFS
      // input method here.
      //TODO: this is inefficient - hadoop input format does not support auto scaling!
      import com.spotify.scio.hdfs._
      sc.hdfsTextFile(path).map(s => CoderUtils.decodeFromBase64(coder, s))
    }

    /** Returns a Scio's [[SCollection]] based on the data from this [[RDD]]. */
    def toSCollection(argv: Array[String]): SCollection[T] = {
      val (sc, _) = ContextAndArgs(argv)
      self.toSCollection(sc)
    }
  }

  implicit class TextTapToRDD[T: ClassTag](val self: Tap[T])(implicit ev: T <:< String) {

    /** Open data set as a [[RDD]]. */
    def open(spark: SparkContext): RDD[String] = {
      val path = self.asInstanceOf[TextTap].path
      spark.textFile(path)
    }
  }

  implicit class BigQueryTapToRDD[T: ClassTag](val self: Tap[T])
                                              (implicit ev: T <:< HasAnnotation) {

    /** Open data set as a [[DataFrame]]. */
    def open(spark: SparkContext): DataFrame = {

      val table = self.parent.get.asInstanceOf[BigQueryTap].table

      import com.spotify.spark.bigquery._
      SparkSession.builder().getOrCreate().sqlContext.bigQueryTable(table)
    }
  }

  implicit class TypedBigQueryTapToRDD[T: TypeTag: ClassTag](val self: Tap[T])
                                                            (implicit ev: T <:< HasAnnotation) {

    /** Open data set as a BigQuery typed [[RDD]]. */
    def typedOpen(spark: SparkContext): RDD[T] = {
      self.open(spark).rdd
        .map { r =>
          // TODO: validate: what if nested records
          r.schema.fieldNames.foldLeft(new TableRow())((bqRow, n) => bqRow.set(n, r.getAs[Any](n)))
        }
        .map(BigQueryType[T].fromTableRow)
    }
  }

}
