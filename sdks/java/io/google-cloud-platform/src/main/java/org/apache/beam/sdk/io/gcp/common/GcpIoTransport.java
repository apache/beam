/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.common;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.util.NullCredentialInitializer;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.Transport;

/**
 * Google Cloud Platform IO client builders.
 */
public class GcpIoTransport {

  /**
   * Returns a BigQuery client builder using the specified {@link BigQueryOptions}.
   */
  public static Bigquery.Builder
      newBigQueryClient(BigQueryOptions options) {
    return new Bigquery.Builder(Transport.getTransport(), Transport.getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log 404. It clutters the output and is possibly even required by the caller.
            new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  /**
   * Returns a Pubsub client builder using the specified {@link PubsubOptions}.
   *
   * @deprecated Use a {@link org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory}.
   */
  @Deprecated
  public static Pubsub.Builder
      newPubsubClient(PubsubOptions options) {
    return new Pubsub.Builder(Transport.getTransport(), Transport.getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log 404. It clutters the output and is possibly even required by the caller.
            new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setRootUrl(options.getPubsubRootUrl())
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credentials credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      return new ChainingHttpRequestInitializer(
          new NullCredentialInitializer(), httpRequestInitializer);
    } else {
      return new ChainingHttpRequestInitializer(
          new HttpCredentialsAdapter(credential),
          httpRequestInitializer);
    }
  }
}
