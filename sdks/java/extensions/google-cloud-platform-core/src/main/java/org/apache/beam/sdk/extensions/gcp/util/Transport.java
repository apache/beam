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

import static org.apache.beam.sdk.extensions.gcp.options.GcsOptions.GcsCustomAuditEntries.CUSTOM_AUDIT_JOB_ENTRY_KEY;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
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
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
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

  /**
   * Wraps an {@link HttpRequestInitializer} so that HTTP execute and response interceptors
   * increment {@link Counter} instances pre-bound to the given {@link MetricsContainer}. This
   * guarantees that GCS HTTP metrics are attributed directly to the step that created the channel,
   * even when requests execute on background worker threads.
   *
   * <p>The counters are exhaustive, so that a report can be checked for consistency:
   *
   * <ul>
   *   <li>{@code request_count} counts every attempt, retries included, because the request
   *       interceptor runs once per attempt.
   *   <li>Every attempt ends up in exactly one of {@code status_2xx}, {@code status_3xx}, {@code
   *       status_4xx}, {@code status_5xx}, {@code status_other} (1xx) or {@code
   *       request_no_response} (the attempt failed before a response was received, and was either
   *       retried or propagated). Their sum equals {@code request_count}.
   *   <li>For reads, every attempt is also classified by shape into {@code request_count_ranged} (a
   *       GET with a Range header), {@code request_count_unbounded} (a GET without one) or {@code
   *       request_count_other} (anything that is not a GET, e.g. a batched metadata POST). Their
   *       sum equals {@code request_count} as well. Writes are not classified this way, they are
   *       POSTs and PUTs by construction.
   * </ul>
   *
   * <p>Note that {@code request_count_unbounded} counts metadata GETs as well as full object reads,
   * since neither carries a Range header.
   */
  public static HttpRequestInitializer withMetricsContainer(
      HttpRequestInitializer base, @Nullable MetricsContainer container, boolean isWrite) {
    if (container == null) {
      return base;
    }

    String prefix = isWrite ? "gcs_http_write_" : "gcs_http_read_";

    // Pre-resolve counters on the calling thread (e.g. DoFn thread) while it is in the target step
    Counter requestCount =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "request_count"));
    Counter rangeRequestCount =
        isWrite
            ? null
            : container.getCounter(
                MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "request_count_ranged"));
    Counter unboundedStreamCount =
        isWrite
            ? null
            : container.getCounter(
                MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "request_count_unbounded"));
    // Requests that are not a GET, so that the shape counters above add up to request_count.
    Counter otherRequestCount =
        isWrite
            ? null
            : container.getCounter(
                MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "request_count_other"));
    Counter status2xx =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "status_2xx"));
    // 3xx is not an error for GCS: a resumable upload answers 308 to every chunk but the last.
    Counter status3xx =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "status_3xx"));
    Counter status4xx =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "status_4xx"));
    Counter status5xx =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "status_5xx"));
    Counter statusOther =
        container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "status_other"));
    Counter noResponse =
        container.getCounter(
            MetricName.named(GcsUtil.METRIC_NAMESPACE, prefix + "request_no_response"));

    return request -> {
      base.initialize(request);
      HttpExecuteInterceptor existingExecuteInterceptor = request.getInterceptor();
      request.setInterceptor(
          req -> {
            if (existingExecuteInterceptor != null) {
              existingExecuteInterceptor.intercept(req);
            }
            recordRequestMetrics(
                req, requestCount, rangeRequestCount, unboundedStreamCount, otherRequestCount);
          });

      HttpResponseInterceptor existingResponseInterceptor = request.getResponseInterceptor();
      request.setResponseInterceptor(
          res -> {
            if (existingResponseInterceptor != null) {
              existingResponseInterceptor.interceptResponse(res);
            }
            recordResponseMetrics(res, status2xx, status3xx, status4xx, status5xx, statusOther);
          });

      // An attempt that throws before a response is received never reaches the response
      // interceptor, so it is counted here instead. The existing handler decides whether the
      // request is retried, this only observes it.
      HttpIOExceptionHandler existingIOExceptionHandler = request.getIOExceptionHandler();
      request.setIOExceptionHandler(
          (req, supportsRetry) -> {
            noResponse.inc();
            return existingIOExceptionHandler != null
                && existingIOExceptionHandler.handleIOException(req, supportsRetry);
          });
    };
  }

  private static HttpRequestInitializer httpRequestInitializerFromOptions(GcsOptions options) {
    // Do not log the code 404. Code up the stack will deal with 404's if needed,
    // and logging it by default clutters the output during file staging.
    RetryHttpRequestInitializer retryHttpRequestInitializer =
        new RetryHttpRequestInitializer(ImmutableList.of(404), new UploadIdResponseInterceptor());

    // Set custom audit info in request headers
    String jobName = Optional.ofNullable(options.getJobName()).orElse("UNKNOWN");

    ImmutableMap.Builder<String, String> builder =
        new ImmutableMap.Builder<String, String>().put(CUSTOM_AUDIT_JOB_ENTRY_KEY, jobName);

    Map<String, String> customAuditEntries = options.getGcsCustomAuditEntries();
    if (customAuditEntries != null && customAuditEntries.size() > 0) {
      builder.putAll(customAuditEntries);
    }

    // Note: Custom audit entries with "job" key will overwrite the default above
    retryHttpRequestInitializer.setHttpHeaders(builder.buildKeepingLast());

    @Nullable Integer readTimeout = options.getGcsHttpRequestReadTimeout();
    if (readTimeout != null) {
      retryHttpRequestInitializer.setReadTimeout(readTimeout);
    }
    @Nullable Integer writeTimeout = options.getGcsHttpRequestWriteTimeout();
    if (writeTimeout != null) {
      retryHttpRequestInitializer.setWriteTimeout(writeTimeout);
    }
    Credentials credential = options.getGcpCredential();
    HttpRequestInitializer credentialsInitializer =
        credential == null
            ? new NullCredentialInitializer()
            : new HttpCredentialsAdapter(credential);

    return new ChainingHttpRequestInitializer(credentialsInitializer, retryHttpRequestInitializer);
  }

  private static void recordRequestMetrics(
      HttpRequest req,
      Counter requestCount,
      @Nullable Counter rangeRequestCount,
      @Nullable Counter unboundedStreamCount,
      @Nullable Counter otherRequestCount) {
    String method = req.getRequestMethod();
    requestCount.inc();
    if ("GET".equalsIgnoreCase(method)) {
      String range = req.getHeaders() != null ? req.getHeaders().getRange() : null;
      if (range != null) {
        if (rangeRequestCount != null) {
          rangeRequestCount.inc();
        }
      } else {
        if (unboundedStreamCount != null) {
          unboundedStreamCount.inc();
        }
      }
    } else if (otherRequestCount != null) {
      // Not a GET, e.g. the POST of a batched metadata lookup. Counted so that the three shape
      // counters add up to requestCount.
      otherRequestCount.inc();
    }
  }

  private static void recordResponseMetrics(
      HttpResponse res,
      Counter status2xx,
      Counter status3xx,
      Counter status4xx,
      Counter status5xx,
      Counter statusOther) {
    int code = res.getStatusCode();
    if (code >= 200 && code < 300) {
      status2xx.inc();
    } else if (code >= 300 && code < 400) {
      // Not an error: a resumable upload answers 308 Resume Incomplete to every chunk but the last.
      status3xx.inc();
    } else if (code >= 400 && code < 500) {
      status4xx.inc();
    } else if (code >= 500 && code < 600) {
      status5xx.inc();
    } else {
      statusOther.inc();
    }
  }
}
