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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler.BackOffRequired;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpMediaType;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that helps write {@link SplunkEvent} records to Splunk's Http Event Collector
 * (HEC) endpoint.
 */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class HttpEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(HttpEventPublisher.class);

  private static final int DEFAULT_MAX_CONNECTIONS = 1;

  private static final boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;

  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  @VisibleForTesting static final String HEC_URL_PATH = "services/collector/event";

  private static final HttpMediaType MEDIA_TYPE =
      new HttpMediaType("application/json;profile=urn:splunk:event:1.0;charset=utf-8");

  private static final String CONTENT_TYPE =
      Joiner.on('/').join(MEDIA_TYPE.getType(), MEDIA_TYPE.getSubType());

  private static final String AUTHORIZATION_SCHEME = "Splunk %s";

  private static final String HTTPS_PROTOCOL_PREFIX = "https";

  /** Provides a builder for creating a {@link HttpEventPublisher}. */
  static Builder newBuilder() {
    return new AutoValue_HttpEventPublisher.Builder();
  }

  abstract ApacheHttpTransport transport();

  abstract HttpRequestFactory requestFactory();

  abstract GenericUrl genericUrl();

  abstract String token();

  abstract @Nullable Integer maxElapsedMillis();

  @SuppressWarnings("mutable")
  abstract byte @Nullable [] rootCaCertificate();

  abstract Boolean disableCertificateValidation();

  abstract Boolean enableGzipHttpCompression();

  /**
   * Executes a POST for the list of {@link SplunkEvent} objects into Splunk's Http Event Collector
   * endpoint.
   *
   * @param events list of {@link SplunkEvent}s
   * @return {@link HttpResponse} for the POST
   */
  HttpResponse execute(List<SplunkEvent> events) throws IOException {

    HttpContent content = getContent(events);
    HttpRequest request = requestFactory().buildPostRequest(genericUrl(), content);

    if (enableGzipHttpCompression()) {
      request.setEncoding(new GZipEncoding());
    }

    HttpBackOffUnsuccessfulResponseHandler responseHandler =
        new HttpBackOffUnsuccessfulResponseHandler(getConfiguredBackOff());

    responseHandler.setBackOffRequired(BackOffRequired.ON_SERVER_ERROR);

    request.setUnsuccessfulResponseHandler(responseHandler);
    HttpIOExceptionHandler ioExceptionHandler =
        new HttpBackOffIOExceptionHandler(getConfiguredBackOff());
    request.setIOExceptionHandler(ioExceptionHandler);
    setHeaders(request, token());

    return request.execute();
  }

  /**
   * Same as {@link HttpEventPublisher#execute(List)} but with a single {@link SplunkEvent}.
   *
   * @param event {@link SplunkEvent} object
   */
  HttpResponse execute(SplunkEvent event) throws IOException {
    return this.execute(ImmutableList.of(event));
  }

  /**
   * Returns an {@link ExponentialBackOff} with the right settings.
   *
   * @return {@link ExponentialBackOff} object
   */
  @VisibleForTesting
  ExponentialBackOff getConfiguredBackOff() {
    return new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(maxElapsedMillis()).build();
  }

  /** Shutsdown connection manager and releases all resources. */
  void close() throws IOException {
    if (transport() != null) {
      LOG.info("Closing publisher transport.");
      transport().shutdown();
    }
  }

  /**
   * Utility method to set Authorization and other relevant http headers into the {@link
   * HttpRequest}.
   *
   * @param request {@link HttpRequest} object to add headers to
   * @param token Splunk's HEC authorization token
   */
  private void setHeaders(HttpRequest request, String token) {
    request.getHeaders().setAuthorization(String.format(AUTHORIZATION_SCHEME, token));

    if (enableGzipHttpCompression()) {
      request.getHeaders().setContentEncoding("gzip");
    }
  }

  /**
   * Marshals a list of {@link SplunkEvent}s into an {@link HttpContent} object that can be used to
   * create an {@link HttpRequest}.
   *
   * @param events list of {@link SplunkEvent}s
   * @return {@link HttpContent} that can be used to create an {@link HttpRequest}.
   */
  @VisibleForTesting
  HttpContent getContent(List<SplunkEvent> events) {
    String payload = getStringPayload(events);
    LOG.debug("Payload content: {}", payload);
    return ByteArrayContent.fromString(CONTENT_TYPE, payload);
  }

  /** Extracts the payload string from a list of {@link SplunkEvent}s. */
  @VisibleForTesting
  String getStringPayload(List<SplunkEvent> events) {
    StringBuilder sb = new StringBuilder();
    events.forEach(event -> sb.append(GSON.toJson(event)));
    return sb.toString();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTransport(ApacheHttpTransport transport);

    abstract ApacheHttpTransport transport();

    abstract Builder setRequestFactory(HttpRequestFactory requestFactory);

    abstract Builder setGenericUrl(GenericUrl genericUrl);

    abstract GenericUrl genericUrl();

    abstract Builder setToken(String token);

    abstract String token();

    abstract Builder setDisableCertificateValidation(Boolean disableCertificateValidation);

    abstract Boolean disableCertificateValidation();

    abstract Builder setRootCaCertificate(byte[] certificate);

    abstract byte[] rootCaCertificate();

    abstract Builder setEnableGzipHttpCompression(Boolean enableGzipHttpCompression);

    abstract Builder setMaxElapsedMillis(Integer maxElapsedMillis);

    abstract Integer maxElapsedMillis();

    abstract HttpEventPublisher autoBuild();

    /**
     * Method to set the Splunk Http Event Collector URL.
     *
     * @param url event collector URL
     * @return {@link Builder}
     */
    Builder withUrl(String url) throws UnsupportedEncodingException {
      checkNotNull(url, "withUrl(url) called with null input.");
      return setGenericUrl(getGenericUrl(url));
    }

    /**
     * Method to set the Splunk Http Event Collector authentication token.
     *
     * @param token HEC's authentication token.
     * @return {@link Builder}
     */
    Builder withToken(String token) {
      checkNotNull(token, "withToken(token) called with null input.");
      return setToken(token);
    }

    /**
     * Method to disable SSL certificate validation. Defaults to {@value
     * DEFAULT_DISABLE_CERTIFICATE_VALIDATION}.
     *
     * @param disableCertificateValidation whether to disable SSL certificate validation.
     * @return {@link Builder}
     */
    Builder withDisableCertificateValidation(Boolean disableCertificateValidation) {
      checkNotNull(
          disableCertificateValidation,
          "withDisableCertificateValidation(disableCertificateValidation) called with null input.");
      return setDisableCertificateValidation(disableCertificateValidation);
    }

    /**
     * Method to set the root CA certificate.
     *
     * @param certificate User provided root CA certificate
     * @return {@link Builder}
     */
    public Builder withRootCaCertificate(byte[] certificate) {
      checkNotNull(certificate, "withRootCaCertificate(certificate) called with null input.");
      return setRootCaCertificate(certificate);
    }

    /**
     * Method to specify if HTTP requests sent to Splunk HEC should be GZIP encoded.
     *
     * @param enableGzipHttpCompression whether to enable Gzip encoding.
     * @return {@link Builder}
     */
    public Builder withEnableGzipHttpCompression(Boolean enableGzipHttpCompression) {
      checkNotNull(
          enableGzipHttpCompression,
          "withEnableGzipHttpCompression(enableGzipHttpCompression) called with null input.");
      return setEnableGzipHttpCompression(enableGzipHttpCompression);
    }

    /**
     * Method to max timeout for {@link ExponentialBackOff}. Otherwise uses the default setting for
     * {@link ExponentialBackOff}.
     *
     * @param maxElapsedMillis max elapsed time in milliseconds for timeout.
     * @return {@link Builder}
     */
    Builder withMaxElapsedMillis(Integer maxElapsedMillis) {
      checkNotNull(
          maxElapsedMillis, "withMaxElapsedMillis(maxElapsedMillis) called with null input.");
      return setMaxElapsedMillis(maxElapsedMillis);
    }

    /**
     * Validates and builds a {@link HttpEventPublisher} object.
     *
     * @return {@link HttpEventPublisher}
     */
    HttpEventPublisher build()
        throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
            CertificateException {

      checkNotNull(token(), "Authentication token needs to be specified via withToken(token).");
      checkNotNull(genericUrl(), "URL needs to be specified via withUrl(url).");

      if (disableCertificateValidation() == null) {
        LOG.info("Certificate validation disabled: {}", DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
        setDisableCertificateValidation(DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
      }

      if (maxElapsedMillis() == null) {
        LOG.info(
            "Defaulting max backoff time to: {} milliseconds ",
            ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
        setMaxElapsedMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
      }

      CloseableHttpClient httpClient =
          getHttpClient(
              DEFAULT_MAX_CONNECTIONS, disableCertificateValidation(), rootCaCertificate());

      setTransport(new ApacheHttpTransport(httpClient));
      setRequestFactory(transport().createRequestFactory());

      return autoBuild();
    }

    /**
     * Converts a baseUrl into a {@link GenericUrl}.
     *
     * @param baseUrl url pointing to the hec endpoint.
     * @return {@link GenericUrl}
     */
    GenericUrl getGenericUrl(String baseUrl) {
      String url = Joiner.on('/').join(baseUrl, HEC_URL_PATH);

      return new GenericUrl(url);
    }

    /**
     * Creates a {@link CloseableHttpClient} to make HTTP POSTs against Splunk's HEC.
     *
     * @param maxConnections max number of parallel connections
     * @param disableCertificateValidation should disable certificate validation
     * @param rootCaCertificate root CA certificate
     */
    private CloseableHttpClient getHttpClient(
        int maxConnections, boolean disableCertificateValidation, byte[] rootCaCertificate)
        throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
            CertificateException {

      HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

      if (genericUrl().getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX)) {
        LOG.info("SSL connection requested");

        HostnameVerifier hostnameVerifier =
            disableCertificateValidation
                ? NoopHostnameVerifier.INSTANCE
                : new DefaultHostnameVerifier();

        SSLContext sslContext = SSLContextBuilder.create().build();
        if (disableCertificateValidation) {
          LOG.info("Certificate validation is disabled");
          sslContext =
              SSLContextBuilder.create()
                  .loadTrustMaterial((TrustStrategy) (chain, authType) -> true)
                  .build();
        } else if (rootCaCertificate != null) {
          LOG.info("Self-Signed Certificate provided");
          InputStream inStream = new ByteArrayInputStream(rootCaCertificate);
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          X509Certificate cert = (X509Certificate) cf.generateCertificate(inStream);
          CustomX509TrustManager customTrustManager = new CustomX509TrustManager(cert);
          sslContext.init(null, new TrustManager[] {customTrustManager}, null);
        }

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
