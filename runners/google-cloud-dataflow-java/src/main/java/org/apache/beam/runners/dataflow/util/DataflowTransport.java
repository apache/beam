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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.sdk.extensions.gcp.util.Transport.getJsonFactory;
import static org.apache.beam.sdk.extensions.gcp.util.Transport.getTransport;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.dataflow.Dataflow;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Helpers for cloud communication. */
public class DataflowTransport {

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
      String rootUrl =
          url.getProtocol()
              + "://"
              + url.getHost()
              + (url.getPort() > 0 ? ":" + url.getPort() : "");
      return new ApiComponents(rootUrl, url.getPath());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid URL: " + urlString);
    }
  }

  /** Returns a Google Cloud Dataflow client builder. */
  public static Dataflow.Builder newDataflowClient(DataflowPipelineOptions options) {
    String servicePath = options.getDataflowEndpoint();
    ApiComponents components;
    if (servicePath.contains("://")) {
      components = apiComponentsFromUrl(servicePath);
    } else {
      components = new ApiComponents(options.getApiRootUrl(), servicePath);
    }

    return new Dataflow.Builder(
            getTransport(),
            getJsonFactory(),
            chainHttpRequestInitializer(
                options.getGcpCredential(),
                // Do not log 404. It clutters the output and is possibly even required by the
                // caller.
                new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setApplicationName(options.getAppName())
        .setRootUrl(components.rootUrl)
        .setServicePath(components.servicePath)
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credentials credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      NullCredentialInitializer.throwNullCredentialException();
    }
    return new ChainingHttpRequestInitializer(
        new HttpCredentialsAdapter(credential), httpRequestInitializer);
  }
}
