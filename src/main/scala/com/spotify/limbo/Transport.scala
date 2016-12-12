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

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.http.HttpRequestInitializer
import com.google.api.services.dataproc.{Dataproc => GDataproc}
import com.google.cloud.dataflow.sdk.util.{RetryHttpRequestInitializer, Transport => GTransport}
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer
import com.google.common.collect.ImmutableList

object Transport {

  private[limbo] def chainHttpRequestInitializer(credential: Credential,
                                                 httpRequestInitializer: HttpRequestInitializer)
  : HttpRequestInitializer = {
    if (credential == null) {
      httpRequestInitializer
    } else {
      new ChainingHttpRequestInitializer(credential, httpRequestInitializer)
    }
  }

  def newDataprocClient(credential: Credential, applicationName: String): GDataproc = {
    new GDataproc.Builder(
      GTransport.getTransport,
      GTransport.getJsonFactory,
      chainHttpRequestInitializer(credential,
        // Do not log 404. It clutters the output and is possibly even required by the caller.
        new RetryHttpRequestInitializer(ImmutableList.of(Integer.valueOf(404)))))
      .setApplicationName(applicationName)
      .build()
  }

}
