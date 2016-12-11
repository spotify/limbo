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
import org.slf4j.LoggerFactory

object Limbo {

  private val logger = LoggerFactory.getLogger(Limbo.getClass)

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val scol = sc.parallelize(1 to 2)

    val rdd = scol.toRDD()
    rdd
      .map(_ * 2)
      .saveAsTextFile(args("output"))
  }
}
