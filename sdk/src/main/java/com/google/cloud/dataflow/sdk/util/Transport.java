/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.services.AbstractGoogleClient.Builder;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.common.base.MoreObjects;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * Helpers for cloud communication.
 */
public class Transport {

  private static class SingletonHelper {
    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY;

    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT;

    static {
      try {
        JSON_FACTORY = JacksonFactory.getDefaultInstance();
        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static HttpTransport getTransport() {
    return SingletonHelper.HTTP_TRANSPORT;
  }

  public static JsonFactory getJsonFactory() {
    return SingletonHelper.JSON_FACTORY;
  }

  /**
   * Returns a BigQuery client builder.
   * <p>
   * Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Bigquery.Builder
      newBigQueryClient(BigQueryOptions options) {
    return new Bigquery.Builder(getTransport(), getJsonFactory(),
        new RetryHttpRequestInitializer(options.getGcpCredential()))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(
            new ChainedGoogleClientRequestInitializer(options.getGoogleApiTrace()));
  }

  /**
   * Returns a Pubsub client builder.
   * <p>
   * Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Pubsub.Builder
      newPubsubClient(StreamingOptions options) {
    return new Pubsub.Builder(getTransport(), getJsonFactory(),
        new RetryHttpRequestInitializer(options.getGcpCredential()))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(
            new ChainedGoogleClientRequestInitializer(options.getGoogleApiTrace()));
  }

  /**
   * Returns a Google Cloud Dataflow client builder.
   */
  public static Dataflow.Builder newDataflowClient(DataflowPipelineOptions options) {
    String rootUrl = options.getApiRootUrl();
    String servicePath = options.getDataflowEndpoint();
    if (servicePath.contains("://")) {
      try {
        URL url = new URL(servicePath);
        rootUrl = url.getProtocol() + "://" + url.getHost() +
            (url.getPort() > 0 ? ":" + url.getPort() : "");
        servicePath = url.getPath();
      } catch (MalformedURLException e) {
        throw new RuntimeException("Invalid URL: " + servicePath);
      }
    }

    return new Dataflow.Builder(getTransport(),
        getJsonFactory(),
        new RetryHttpRequestInitializer(options.getGcpCredential()))
        .setApplicationName(options.getAppName())
        .setRootUrl(rootUrl)
        .setServicePath(servicePath)
        .setGoogleClientRequestInitializer(
            new ChainedGoogleClientRequestInitializer(options.getGoogleApiTrace()));
  }

  /**
   * Returns a Dataflow client which does not automatically retry failed
   * requests.
   */
  public static Dataflow.Builder
      newRawDataflowClient(DataflowPipelineOptions options) {
    return newDataflowClient(options)
        .setHttpRequestInitializer(options.getGcpCredential())
        .setGoogleClientRequestInitializer(
            new ChainedGoogleClientRequestInitializer(options.getGoogleApiTrace()));
  }

  /**
   * Returns a Cloud Storage client builder.
   * <p>
   * Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Storage.Builder
      newStorageClient(GcsOptions options) {
    return new Storage.Builder(getTransport(), getJsonFactory(),
        new RetryHttpRequestInitializer(
            // Do not log the code 404. Code up the stack will deal with 404's if needed, and
            // logging it by default clutters the output during file staging.
            options.getGcpCredential(), NanoClock.SYSTEM, Sleeper.DEFAULT, Arrays.asList(404)))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(
            new ChainedGoogleClientRequestInitializer(options.getGoogleApiTrace()));
  }

  /**
   * Allows multiple {@link GoogleClientRequestInitializer}s to be chained together for use with
   * {@link Builder}.
   */
  private static final class ChainedGoogleClientRequestInitializer
      implements GoogleClientRequestInitializer {
    private static final GoogleClientRequestInitializer[] EMPTY_ARRAY =
        new GoogleClientRequestInitializer[]{};
    private final GoogleClientRequestInitializer[] chain;

    private ChainedGoogleClientRequestInitializer(GoogleClientRequestInitializer... initializer) {
      this.chain = MoreObjects.firstNonNull(initializer, EMPTY_ARRAY);
    }

    @Override
    public void initialize(AbstractGoogleClientRequest<?> request) throws IOException {
      for (GoogleClientRequestInitializer initializer : chain) {
        initializer.initialize(request);
      }
    }
  }
}
