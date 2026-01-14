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
package org.apache.beam.sdk.io.datadog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpMediaType;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DatadogEventPublisher} is a utility class that helps write {@link DatadogEvent}s to a
 * Datadog Logs API endpoint.
 */
@AutoValue
public abstract class DatadogEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(DatadogEventPublisher.class);

  private static final int DEFAULT_MAX_CONNECTIONS = 1;

  @VisibleForTesting protected static final String DD_URL_PATH = "api/v2/logs";

  private static final String DD_API_KEY_HEADER = "dd-api-key";

  private static final String DD_ORIGIN_HEADER = "dd-evp-origin";
  private static final String DD_ORIGIN_DATAFLOW = "dataflow";

  private static final HttpMediaType MEDIA_TYPE =
      new HttpMediaType("application/json;charset=utf-8");

  private static final String CONTENT_TYPE =
      Joiner.on('/').join(MEDIA_TYPE.getType(), MEDIA_TYPE.getSubType());

  private static final String HTTPS_PROTOCOL_PREFIX = "https";

  public static Builder newBuilder() {
    return new AutoValue_DatadogEventPublisher.Builder();
  }

  abstract ApacheHttpTransport transport();

  abstract HttpRequestFactory requestFactory();

  abstract GenericUrl genericUrl();

  abstract String apiKey();

  abstract Integer maxElapsedMillis();

  /**
   * Executes a POST for the list of {@link DatadogEvent} objects into Datadog's Logs API.
   *
   * @param events List of {@link DatadogEvent}s
   * @return {@link HttpResponse} for the POST.
   */
  public HttpResponse execute(List<DatadogEvent> events) throws IOException {

    HttpContent content = getContent(events);
    HttpRequest request = requestFactory().buildPostRequest(genericUrl(), content);

    request.setEncoding(new GZipEncoding());
    request.setUnsuccessfulResponseHandler(
        new HttpSendLogsUnsuccessfulResponseHandler(getConfiguredBackOff()));
    request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(getConfiguredBackOff()));

    setHeaders(request, apiKey());

    return request.execute();
  }

  /**
   * Same as {@link DatadogEventPublisher#execute(List)} but with a single {@link DatadogEvent}.
   *
   * @param event {@link DatadogEvent} object.
   */
  public HttpResponse execute(DatadogEvent event) throws IOException {
    return this.execute(ImmutableList.of(event));
  }

  /**
   * Return an {@link ExponentialBackOff} with the right settings.
   *
   * @return {@link ExponentialBackOff} object.
   */
  @VisibleForTesting
  protected ExponentialBackOff getConfiguredBackOff() {
    return new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(maxElapsedMillis()).build();
  }

  /** Shutdown connection manager and releases all resources. */
  public void close() throws IOException {
    if (transport() != null) {
      LOG.info("Closing publisher transport.");
      transport().shutdown();
    }
  }

  /**
   * Utility method to set http headers into the {@link HttpRequest}.
   *
   * @param request {@link HttpRequest} object to add headers to.
   * @param apiKey Datadog's Logs API key.
   */
  private void setHeaders(HttpRequest request, String apiKey) {
    request.getHeaders().set(DD_API_KEY_HEADER, apiKey);
    request.getHeaders().set(DD_ORIGIN_HEADER, DD_ORIGIN_DATAFLOW);
    request.getHeaders().setContentEncoding("gzip");
  }

  /**
   * Utility method to marshall a list of {@link DatadogEvent}s into an {@link HttpContent} object
   * that can be used to create an {@link HttpRequest}.
   *
   * @param events List of {@link DatadogEvent}s
   * @return {@link HttpContent} that can be used to create an {@link HttpRequest}.
   */
  @VisibleForTesting
  protected HttpContent getContent(List<DatadogEvent> events) {
    String payload = DatadogEventSerializer.getPayloadString(events);
    LOG.debug("Payload content: {}", payload);
    return ByteArrayContent.fromString(CONTENT_TYPE, payload);
  }

  static class HttpSendLogsUnsuccessfulResponseHandler implements HttpUnsuccessfulResponseHandler {
    /*
      See: https://docs.datadoghq.com/api/latest/logs/#send-logs
      408: Request Timeout, request should be retried after some time
      429: Too Many Requests, request should be retried after some time
    */
    private static final Set<Integer> RETRYABLE_4XX_CODES = ImmutableSet.of(408, 429);

    private final Sleeper sleeper = Sleeper.DEFAULT;
    private final BackOff backOff;

    HttpSendLogsUnsuccessfulResponseHandler(BackOff backOff) {
      this.backOff = Preconditions.checkNotNull(backOff);
    }

    @Override
    public boolean handleResponse(HttpRequest req, HttpResponse res, boolean supportsRetry)
        throws IOException {
      if (!supportsRetry) {
        return false;
      }

      boolean is5xxStatusCode = res.getStatusCode() / 100 == 5;
      boolean isRetryable4xxStatusCode = RETRYABLE_4XX_CODES.contains(res.getStatusCode());
      if (is5xxStatusCode || isRetryable4xxStatusCode) {
        try {
          return BackOffUtils.next(sleeper, backOff);
        } catch (InterruptedException exception) {
          // Mark thread as interrupted since we cannot throw InterruptedException here.
          Thread.currentThread().interrupt();
        }
      }
      return false;
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTransport(ApacheHttpTransport transport);

    abstract ApacheHttpTransport transport();

    abstract Builder setRequestFactory(HttpRequestFactory requestFactory);

    abstract Builder setGenericUrl(GenericUrl genericUrl);

    abstract GenericUrl genericUrl();

    abstract Builder setApiKey(String apiKey);

    abstract String apiKey();

    abstract Builder setMaxElapsedMillis(Integer maxElapsedMillis);

    abstract Integer maxElapsedMillis();

    abstract DatadogEventPublisher autoBuild();

    /**
     * Method to set the Datadog Logs API URL.
     *
     * @param url Logs API URL
     * @return {@link Builder}
     */
    public Builder withUrl(String url) throws UnsupportedEncodingException {
      checkNotNull(url, "withUrl(url) called with null input.");
      return setGenericUrl(getGenericUrl(url));
    }

    /**
     * Method to set the Datadog Logs API key.
     *
     * @param apiKey Logs API key.
     * @return {@link Builder}
     */
    public Builder withApiKey(String apiKey) {
      checkNotNull(apiKey, "withApiKey(apiKey) called with null input.");
      return setApiKey(apiKey);
    }

    /**
     * Method to max timeout for {@link ExponentialBackOff}. Otherwise uses the default setting for
     * {@link ExponentialBackOff}.
     *
     * @param maxElapsedMillis max elapsed time in milliseconds for timeout.
     * @return {@link Builder}
     */
    public Builder withMaxElapsedMillis(Integer maxElapsedMillis) {
      checkNotNull(
          maxElapsedMillis, "withMaxElapsedMillis(maxElapsedMillis) called with null input.");
      return setMaxElapsedMillis(maxElapsedMillis);
    }

    /**
     * Validates and builds a {@link DatadogEventPublisher} object.
     *
     * @return {@link DatadogEventPublisher}
     */
    public DatadogEventPublisher build() throws NoSuchAlgorithmException, KeyManagementException {

      checkNotNull(apiKey(), "API Key needs to be specified via withApiKey(apiKey).");
      checkNotNull(genericUrl(), "URL needs to be specified via withUrl(url).");

      if (maxElapsedMillis() == null) {
        LOG.info(
            "Defaulting max backoff time to: {} milliseconds ",
            ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
        setMaxElapsedMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
      }

      CloseableHttpClient httpClient = getHttpClient(DEFAULT_MAX_CONNECTIONS);

      setTransport(new ApacheHttpTransport(httpClient));
      setRequestFactory(transport().createRequestFactory());

      return autoBuild();
    }

    /**
     * Utility method to convert a baseUrl into a {@link GenericUrl}.
     *
     * @param baseUrl url pointing to the Logs API endpoint.
     * @return {@link GenericUrl}
     */
    private GenericUrl getGenericUrl(String baseUrl) {
      String url = Joiner.on('/').join(baseUrl, DD_URL_PATH);

      return new GenericUrl(url);
    }

    /**
     * Utility method to create a {@link CloseableHttpClient} to make http POSTs against Datadog's
     * Logs API.
     */
    private CloseableHttpClient getHttpClient(int maxConnections)
        throws NoSuchAlgorithmException, KeyManagementException {

      HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

      if (genericUrl().getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX)) {
        LOG.info("SSL connection requested");

        HostnameVerifier hostnameVerifier = new DefaultHostnameVerifier();

        SSLContext sslContext = SSLContextBuilder.create().build();

        SSLConnectionSocketFactory connectionSocketFactory =
            new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        builder.setSSLSocketFactory(connectionSocketFactory);
      }

      builder.setMaxConnTotal(maxConnections);
      builder.setDefaultRequestConfig(
          RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());

      return builder.build();
    }
  }
}
