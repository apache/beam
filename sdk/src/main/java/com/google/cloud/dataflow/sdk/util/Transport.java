/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.clouddebugger.v2.Clouddebugger;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.storage.Storage;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;

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

  private static class ApiComponents {
    public String rootUrl;
    public String servicePath;

    public ApiComponents(String root, String path) {
      this.rootUrl = root;
      this.servicePath = path;
    }
  }

  private static ApiComponents apiComponentsFromUrl(String urlString) {
    try {
      URL url = new URL(urlString);
      String rootUrl = url.getProtocol() + "://" + url.getHost() +
          (url.getPort() > 0 ? ":" + url.getPort() : "");
      return new ApiComponents(rootUrl, url.getPath());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid URL: " + urlString);
    }
  }

  /**
   * Returns a BigQuery client builder.
   *
   * <p>Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Bigquery.Builder
      newBigQueryClient(BigQueryOptions options) {
    return new Bigquery.Builder(getTransport(), getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log 404. It clutters the output and is possibly even required by the caller.
            new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  /**
   * Returns a Pubsub client builder.
   *
   * <p>Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Pubsub.Builder
      newPubsubClient(DataflowPipelineOptions options) {
    return new Pubsub.Builder(getTransport(), getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log 404. It clutters the output and is possibly even required by the caller.
            new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setRootUrl(options.getPubsubRootUrl())
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  /**
   * Returns a Google Cloud Dataflow client builder.
   */
  public static Dataflow.Builder newDataflowClient(DataflowPipelineOptions options) {
    String servicePath = options.getDataflowEndpoint();
    ApiComponents components;
    if (servicePath.contains("://")) {
      components = apiComponentsFromUrl(servicePath);
    } else {
      components = new ApiComponents(options.getApiRootUrl(), servicePath);
    }

    return new Dataflow.Builder(getTransport(),
        getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log 404. It clutters the output and is possibly even required by the caller.
            new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setApplicationName(options.getAppName())
        .setRootUrl(components.rootUrl)
        .setServicePath(components.servicePath)
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  public static Clouddebugger.Builder newClouddebuggerClient(DataflowPipelineOptions options) {
    return new Clouddebugger.Builder(getTransport(),
        getJsonFactory(),
        chainHttpRequestInitializer(options.getGcpCredential(), new RetryHttpRequestInitializer()))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  /**
   * Returns a Dataflow client that does not automatically retry failed
   * requests.
   */
  public static Dataflow.Builder
      newRawDataflowClient(DataflowPipelineOptions options) {
    return newDataflowClient(options)
        .setHttpRequestInitializer(options.getGcpCredential())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  /**
   * Returns a Cloud Storage client builder.
   *
   * <p>Note: this client's endpoint is <b>not</b> modified by the
   * {@link DataflowPipelineDebugOptions#getApiRootUrl()} option.
   */
  public static Storage.Builder
      newStorageClient(GcsOptions options) {
    String servicePath = options.getGcsEndpoint();
    Storage.Builder storageBuilder = new Storage.Builder(getTransport(), getJsonFactory(),
        chainHttpRequestInitializer(
            options.getGcpCredential(),
            // Do not log the code 404. Code up the stack will deal with 404's if needed, and
            // logging it by default clutters the output during file staging.
            new RetryHttpRequestInitializer(
                ImmutableList.of(404), new UploadIdResponseInterceptor())))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    if (servicePath != null) {
      ApiComponents components = apiComponentsFromUrl(servicePath);
      storageBuilder.setRootUrl(components.rootUrl);
      storageBuilder.setServicePath(components.servicePath);
    }
    return storageBuilder;
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credential credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      return httpRequestInitializer;
    } else {
      return new ChainingHttpRequestInitializer(credential, httpRequestInitializer);
    }
  }
}
