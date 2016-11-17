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
import org.apache.spark.sql.SparkSession
import sh.rav.limbo.util.LimboUtil

object Limbo {

  def main(argv: Array[String]): Unit = {
    // Init - this will be automated:
    val (sc, args) = ContextAndArgs(argv)
    val (sc1, _) = ContextAndArgs(argv)
    val spark = SparkSession.builder().master("local").getOrCreate()

    LimboUtil.configureLocalGCSAccess(spark.sparkContext.hadoopConfiguration)

    try {
      val scol = sc.parallelize(1 to 2)
      val rdd = scol.toRDD(spark).get
      rdd
        .map(_ * 2)
        .toSCollection(sc1)
        .map(_ / 2)
        .saveAsTextFile(args("output"))
      sc1.close()
    } finally {
      // Cleanup
      spark.stop()
    }
  }
}
