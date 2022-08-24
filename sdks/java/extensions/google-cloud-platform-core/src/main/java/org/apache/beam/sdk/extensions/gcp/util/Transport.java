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
package org.apache.beam.sdk.extensions.gcp.util;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.http.annotation.Experimental;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helpers for cloud communication. */
public class Transport {

  private static final Logger LOG = LoggerFactory.getLogger(Transport.class);

  private static @Nullable HttpTransport httpTransport;

  private static class SingletonHelper {
    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY;

    /** Global instance of the HTTP transport. */
    private static final HttpTransport DEFAULT_HTTP_TRANSPORT;

    static {
      try {
        JSON_FACTORY = JacksonFactory.getDefaultInstance();
        DEFAULT_HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get previously set instance of {@link HttpTransport}. If not set, returns the default global
   * instance constructed from {@link GoogleNetHttpTransport#newTrustedTransport()}.
   */
  public static synchronized HttpTransport getTransport() {
    if (httpTransport == null) {
      httpTransport = SingletonHelper.DEFAULT_HTTP_TRANSPORT;
    }
    return httpTransport;
  }

  /**
   * Set a customized instance of {@link HttpTransport} with trust store and mTLS settings. Refer to
   * https://github.com/googleapis/google-auth-library-java#configuring-a-proxy for the construction
   * of customized instance of HttpTransport. Reset to default HttpTransport by passing null to this
   * method.
   */
  @Experimental
  public static synchronized void setHttpTransport(HttpTransport transport) {
    // TDOO(https://github.com/apache/beam/issues/22504) Support customized HttpTransport through
    // pipeline options then remove this experimental method.
    if (transport != null) {
      LOG.warn(
          "Setting customized HttpTransport to {}. Explicitly setting HttpTransport is "
              + "experimental and will possibly be removed in a future version.",
          transport.getClass());
    }

    httpTransport = transport;
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

  /** Returns a Cloud Storage client builder using the specified {@link GcsOptions}. */
  public static Storage.Builder newStorageClient(GcsOptions options) {
    String servicePath = options.getGcsEndpoint();
    Storage.Builder storageBuilder =
        new Storage.Builder(
                getTransport(),
                getJsonFactory(),
                chainHttpRequestInitializer(
                    options.getGcpCredential(),
                    // Do not log the code 404. Code up the stack will deal with 404's if needed,
                    // and
                    // logging it by default clutters the output during file staging.
                    new RetryHttpRequestInitializer(
                        ImmutableList.of(404), new UploadIdResponseInterceptor())))
            .setApplicationName(options.getAppName())
            .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    if (servicePath != null) {
      ApiComponents components = apiComponentsFromUrl(servicePath);
      storageBuilder.setRootUrl(components.rootUrl);
      storageBuilder.setServicePath(components.servicePath);
      storageBuilder.setBatchPath(Paths.get("batch/", components.servicePath).toString());
    }
    return storageBuilder;
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credentials credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      return new ChainingHttpRequestInitializer(
          new NullCredentialInitializer(), httpRequestInitializer);
    } else {
      return new ChainingHttpRequestInitializer(
          new HttpCredentialsAdapter(credential), httpRequestInitializer);
    }
  }
}
