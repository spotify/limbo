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

import com.spotify.scio.testing.SCollectionMatchers
import org.scalatest.{FlatSpec, Matchers}

class LimboImplicitConversionTest
  extends FlatSpec with Matchers with SCollectionMatchers with TestUtils {

  "Conversion" should "support SCollection to RDD trip" in {
    val expected = 1 to 10
    runWithContexts { (scio, spark) =>
      scio.parallelize(1 to 10).toRDD(spark).collect() should contain theSameElementsAs expected
    }
  }

  it should "support RDD to SCollection trip" in {
    val expected = 1 to 10
    runWithContexts { (scio, spark) =>
      spark.parallelize(1 to 10).toSCollection(scio) should containInAnyOrder(expected)
    }
  }

  it should "support round trip" in {
    val expected = 1 to 10
    runWithContexts { (scio, spark) =>
      val scio2 = getScioContextForTest()
      scio.parallelize(1 to 10).toRDD(spark).toSCollection(scio2) should
        containInAnyOrder(expected)
    }
  }

}
