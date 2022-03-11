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
package org.apache.beam.sdk.io.cdap.zendesk.batch.http;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Strings;
import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.cdap.zendesk.batch.ZendeskBatchSourceConfig;
import org.apache.beam.sdk.io.cdap.zendesk.common.ObjectType;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/** A class which contains utilities to build http specific resources. */
public class HttpUtil {

  private static final Map<String, String> SATISFACTION_RATINGS_SCORE_MAP =
      Stream.of(
              new String[][] {
                {"Offered", "offered"},
                {"Unoffered", "unoffered"},
                {"Received", "received"},
                {"Received With Comment", "received_with_comment"},
                {"Received Without Comment", "received_without_comment"},
                {"Good", "good"},
                {"Good With Comment", "good_with_comment"},
                {"Good Without Comment", "good_without_comment"},
                {"Bad", "bad"},
                {"Bad With Comment", "bad_with_comment"},
                {"Bad Without Comment", "bad_without_comment"}
              })
          .collect(Collectors.toMap(data -> data[0], data -> data[1]));

  /**
   * Returns CloseableHttpClient object depending on the batch source config
   *
   * @param config The batch source config
   * @return The instance of CloseableHttpClient object
   */
  public static CloseableHttpClient createHttpClient(ZendeskBatchSourceConfig config) {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    Long connectTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getConnectTimeout());
    Long readTimeoutMillis = TimeUnit.SECONDS.toMillis(config.getReadTimeout());
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setSocketTimeout(readTimeoutMillis.intValue());
    requestBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
    requestBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
    httpClientBuilder.setDefaultRequestConfig(requestBuilder.build());
    return httpClientBuilder.build();
  }

  /**
   * Returns HttpClientContext object depending on the batch source config and url.
   *
   * @param config The batch source config
   * @param url The url
   * @return The instance of HttpClientContext object
   */
  public static HttpClientContext createHttpContext(ZendeskBatchSourceConfig config, String url) {
    String adminEmail = config.getAdminEmail();
    String apiToken = config.getApiToken();
    URI uri = URI.create(url);
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    AuthCache authCache = new BasicAuthCache();
    if (!Strings.isNullOrEmpty(adminEmail)
        && !Strings.isNullOrEmpty(apiToken)
        && !Strings.isNullOrEmpty(url)) {
      HttpHost targetHost = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
      AuthScope authScope = new AuthScope(targetHost);
      credentialsProvider.setCredentials(
          authScope, new UsernamePasswordCredentials(adminEmail + "/token", apiToken));

      authCache.put(targetHost, new BasicScheme());
    }
    HttpClientContext context = HttpClientContext.create();
    context.setCredentialsProvider(credentialsProvider);
    context.setAuthCache(authCache);
    return context;
  }

  /**
   * Creates a url for the first page.
   *
   * @param config The batch source config
   * @param objectType The object type name
   * @param subdomain The subdomain name
   * @param entityId The entity id
   * @return The concatenated url for the first page as per the parameters passed
   */
  public static String createFirstPageUrl(
      ZendeskBatchSourceConfig config, ObjectType objectType, String subdomain, Long entityId) {
    List<String> additionalParams = new ArrayList<>();
    if (objectType.isBatch()) {
      long epochSecond = getEpochSecond(config.getStartDate());
      additionalParams.add(String.format("start_time=%s", epochSecond));
    }
    if (objectType == ObjectType.SATISFACTION_RATINGS) {
      if (!Strings.isNullOrEmpty(config.getStartDate())) {
        long epochSecond = getEpochSecond(config.getStartDate());
        additionalParams.add(String.format("start_time=%s", epochSecond));
      }
      if (!Strings.isNullOrEmpty(config.getEndDate())) {
        long epochSecond = getEpochSecond(config.getEndDate());
        additionalParams.add(String.format("end_time=%s", epochSecond));
      }
      if (!Strings.isNullOrEmpty(config.getSatisfactionRatingsScore())) {
        additionalParams.add(
            String.format(
                "score=%s",
                SATISFACTION_RATINGS_SCORE_MAP.get(config.getSatisfactionRatingsScore())));
      }
    }
    String baseUrl =
        String.format(config.getZendeskBaseUrl(), subdomain, objectType.getApiEndpoint());
    if (entityId != null) {
      baseUrl = String.format(baseUrl, entityId);
    }
    if (!additionalParams.isEmpty()) {
      String additionalParamsString = String.join("&", additionalParams);
      baseUrl =
          String.format(baseUrl.contains("?") ? "%s&%s" : "%s?%s", baseUrl, additionalParamsString);
    }
    return baseUrl;
  }

  /**
   * Returns the Retryer object instance depending on the batch source config.
   *
   * @param config The batch source config
   * @return The instance of Retryer object
   */
  public static Retryer<Map<String, Object>> buildRetryer(ZendeskBatchSourceConfig config) {
    return RetryerBuilder.<Map<String, Object>>newBuilder()
        .retryIfExceptionOfType(RateLimitException.class)
        .withWaitStrategy(
            WaitStrategies.join(
                WaitStrategies.exponentialWait(config.getMaxRetryWait(), TimeUnit.SECONDS),
                WaitStrategies.randomWait(config.getMaxRetryJitterWait(), TimeUnit.MILLISECONDS)))
        .withStopStrategy(StopStrategies.stopAfterAttempt(config.getMaxRetryCount()))
        .build();
  }

  private static long getEpochSecond(String aDate) {
    ZonedDateTime zonedDateTime = ZonedDateTime.parse(aDate, DateTimeFormatter.ISO_DATE_TIME);
    return zonedDateTime.toEpochSecond();
  }
}
