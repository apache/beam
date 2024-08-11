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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Helpers for cloud communication. */
public class Transport {

  private static class SingletonHelper {
    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY;

    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT;

    static {
      try {
        JSON_FACTORY = GsonFactory.getDefaultInstance();
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
    String applicationName =
        String.format(
            "%sapache-beam/%s (GPN:Beam)",
            isNullOrEmpty(options.getAppName()) ? "" : options.getAppName() + " ",
            ReleaseInfo.getReleaseInfo().getSdkVersion());

    String servicePath = options.getGcsEndpoint();

    Storage.Builder storageBuilder =
        new Storage.Builder(
                getTransport(), getJsonFactory(), httpRequestInitializerFromOptions(options))
            .setApplicationName(applicationName)
            .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    if (servicePath != null) {
      ApiComponents components = apiComponentsFromUrl(servicePath);
      storageBuilder.setRootUrl(components.rootUrl);
      storageBuilder.setServicePath(components.servicePath);
      storageBuilder.setBatchPath(Paths.get("batch/", components.servicePath).toString());
    }
    return storageBuilder;
  }

  private static HttpRequestInitializer httpRequestInitializerFromOptions(GcsOptions options) {
    // Do not log the code 404. Code up the stack will deal with 404's if needed,
    // and logging it by default clutters the output during file staging.
    RetryHttpRequestInitializer retryHttpRequestInitializer =
        new RetryHttpRequestInitializer(ImmutableList.of(404), new UploadIdResponseInterceptor());

    // Set custom audit info in request headers
    String jobName = Optional.ofNullable(options.getJobName()).orElse("UNKNOWN");
    retryHttpRequestInitializer.setHttpHeaders(ImmutableMap.of("x-goog-custom-audit-job", jobName));

    @Nullable Integer readTimeout = options.getGcsHttpRequestReadTimeout();
    if (readTimeout != null) {
      retryHttpRequestInitializer.setReadTimeout(readTimeout);
    }
    @Nullable Integer writeTimeout = options.getGcsHttpRequestWriteTimeout();
    if (writeTimeout != null) {
      retryHttpRequestInitializer.setWriteTimeout(writeTimeout);
    }
    Credentials credential = options.getGcpCredential();
    if (credential == null) {
      return new ChainingHttpRequestInitializer(
          new NullCredentialInitializer(), retryHttpRequestInitializer);
    } else {
      return new ChainingHttpRequestInitializer(
          new HttpCredentialsAdapter(credential), retryHttpRequestInitializer);
    }
  }
}
