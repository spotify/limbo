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

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.scalatest.{FlatSpec, Matchers}

class GcpHelpersTest extends FlatSpec with Matchers with TestUtils {

  private def makePropertiesFileWithProject(path: File, projectId: String) {
    val properties = s"""|[core]
                         |account = test-account@google.com
                         |project = $projectId""".stripMargin
    Files.write(properties, path, StandardCharsets.UTF_8)
  }

  private def makePropertiesFileWithProjectAndZone(path: File, projectId: String, zone: String) {
    val properties = s"""|[core]
                         |account = test-account@google.com
                         |project = $projectId
                         |
                         |[compute]
                         |zone = $zone""".stripMargin
    Files.write(properties, path, StandardCharsets.UTF_8)
  }

  "getDefaultZone" should "get zone if present in properties file" in withTempFile { file =>
    val propertyFile = new File(file)
    makePropertiesFileWithProjectAndZone(propertyFile, "testproject", "testzone")
    GcpHelpers.getDefaultZone(propertyFile) should be ("testzone")
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "throw an exception if zone is missing in properties file" in withTempFile { file =>
    val propertyFile = new File(file)
    makePropertiesFileWithProject(propertyFile, "testproject")
    the [Exception] thrownBy {
      GcpHelpers.getDefaultZone(propertyFile)
    } should have message s"Zone not in the config file $propertyFile"
  }
  // scalastyle:on no.whitespace.before.left.bracket

}
