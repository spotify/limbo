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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.cloud.dataflow.sdk.values.TypeDescriptor
import sh.rav.limbo.util.LimboUtil

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.Random

object Limbo {

  implicit class SCollectionToRDD[T: ClassTag](val self: SCollection[T]) {
    def toRDD(spark: SparkSession)
             (minPartitions: Int = spark.sparkContext.defaultParallelism,
              loc: String = null): Future[RDD[T]] = {

      import scala.concurrent.ExecutionContext.Implicits.global

      val coder = self.internal
        .getPipeline.getCoderRegistry.getCoder(TypeDescriptor.of(LimboUtil.classOf[T]))

      self.map(t => CoderUtils.encodeToBase64(coder, t)).saveAsTextFile(loc)
        .map(_ =>
          spark.sparkContext
            .textFile(loc, minPartitions).map(s => CoderUtils.decodeFromBase64(coder, s))
        )
    }
  }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val (sc, arg) = ContextAndArgs(args)
    val spark = SparkSession.builder().master("local").getOrCreate()

    try {
      val scol = sc.parallelize(1 to 10)
      val fRDD = scol.toRDD(spark)(loc = "/tmp/tmp_loc" + Random.nextInt())
      sc.close()
      val r = fRDD.collect{ case rdd => rdd.map(_ * 2).saveAsTextFile("/tmp/doubles") }
      Await.result(r, Duration.Inf)
    } finally {
      spark.stop()
    }
  }
}
