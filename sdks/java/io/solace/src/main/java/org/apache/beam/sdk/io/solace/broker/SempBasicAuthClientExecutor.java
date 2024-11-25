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
package org.apache.beam.sdk.io.solace.broker;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.gson.GsonFactory;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.solace.data.Semp.Queue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class to execute requests to SEMP v2 with Basic Auth authentication.
 *
 * <p>This approach takes advantage of <a
 * href="https://docs.solace.com/Admin/SEMP/SEMP-Security.htm#Sessions">SEMP Sessions</a>. The
 * session is established when a user authenticates with HTTP Basic authentication. When the
 * response is 401 Unauthorized, the client will execute an additional request with Basic Auth
 * header to refresh the token.
 */
public class SempBasicAuthClientExecutor {
  // Every request will be repeated 2 times in case of abnormal connection failures.
  private static final int REQUEST_NUM_RETRIES = 2;
  private static final Map<CookieManagerKey, CookieManager> COOKIE_MANAGER_MAP =
      new ConcurrentHashMap<CookieManagerKey, CookieManager>();
  private static final String COOKIES_HEADER = "Set-Cookie";

  private final String username;
  private final String messageVpn;
  private final String baseUrl;
  private final String password;
  private final CookieManagerKey cookieManagerKey;
  private final transient HttpRequestFactory requestFactory;
  private final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public SempBasicAuthClientExecutor(
      String host,
      String username,
      String password,
      String vpnName,
      HttpRequestFactory httpRequestFactory) {
    this.baseUrl = String.format("%s/SEMP/v2", host);
    this.username = username;
    this.messageVpn = vpnName;
    this.password = password;
    this.requestFactory = httpRequestFactory;
    this.cookieManagerKey = new CookieManagerKey(this.baseUrl, this.username);
    COOKIE_MANAGER_MAP.computeIfAbsent(this.cookieManagerKey, key -> new CookieManager());
  }

  public boolean isQueueNonExclusive(String queueName) throws IOException {
    BrokerResponse response = getQueueResponse(queueName);
    if (response.content == null) {
      throw new IOException("SolaceIO: response from SEMP is empty!");
    }
    Queue q = mapJsonToClass(response.content, Queue.class);
    return q.data().accessType().equals("non-exclusive");
  }

  private static String getQueueEndpoint(String messageVpn, String queueName)
      throws UnsupportedEncodingException {
    return String.format(
        "/monitor/msgVpns/%s/queues/%s", urlEncode(messageVpn), urlEncode(queueName));
  }

  private static String createQueueEndpoint(String messageVpn) throws UnsupportedEncodingException {
    return String.format("/config/msgVpns/%s/queues", urlEncode(messageVpn));
  }

  private static String subscriptionEndpoint(String messageVpn, String queueName)
      throws UnsupportedEncodingException {
    return String.format(
        "/config/msgVpns/%s/queues/%s/subscriptions", urlEncode(messageVpn), urlEncode(queueName));
  }

  BrokerResponse getQueueResponse(String queueName) throws IOException {
    String queryUrl = getQueueEndpoint(messageVpn, queueName);
    HttpResponse response = executeGet(new GenericUrl(baseUrl + queryUrl));
    return BrokerResponse.fromHttpResponse(response);
  }

  BrokerResponse createQueueResponse(String queueName) throws IOException {
    String queryUrl = createQueueEndpoint(messageVpn);
    ImmutableMap<String, Object> params =
        ImmutableMap.<String, Object>builder()
            .put("accessType", "non-exclusive")
            .put("queueName", queueName)
            .put("owner", username)
            .put("permission", "consume")
            .put("ingressEnabled", true)
            .put("egressEnabled", true)
            .build();

    HttpResponse response = executePost(new GenericUrl(baseUrl + queryUrl), params);
    return BrokerResponse.fromHttpResponse(response);
  }

  BrokerResponse createSubscriptionResponse(String queueName, String topicName) throws IOException {
    String queryUrl = subscriptionEndpoint(messageVpn, queueName);

    ImmutableMap<String, Object> params =
        ImmutableMap.<String, Object>builder()
            .put("subscriptionTopic", topicName)
            .put("queueName", queueName)
            .build();
    HttpResponse response = executePost(new GenericUrl(baseUrl + queryUrl), params);
    return BrokerResponse.fromHttpResponse(response);
  }

  private HttpResponse executeGet(GenericUrl url) throws IOException {
    HttpRequest request = requestFactory.buildGetRequest(url);
    return execute(request);
  }

  private HttpResponse executePost(GenericUrl url, ImmutableMap<String, Object> parameters)
      throws IOException {
    HttpContent content = new JsonHttpContent(GsonFactory.getDefaultInstance(), parameters);
    HttpRequest request = requestFactory.buildPostRequest(url, content);
    return execute(request);
  }

  private HttpResponse execute(HttpRequest request) throws IOException {
    request.setNumberOfRetries(REQUEST_NUM_RETRIES);
    HttpHeaders httpHeaders = new HttpHeaders();
    boolean authFromCookie =
        !checkStateNotNull(COOKIE_MANAGER_MAP.get(cookieManagerKey))
            .getCookieStore()
            .getCookies()
            .isEmpty();
    if (authFromCookie) {
      setCookiesFromCookieManager(httpHeaders);
      request.setHeaders(httpHeaders);
    } else {
      httpHeaders.setBasicAuthentication(username, password);
      request.setHeaders(httpHeaders);
    }

    HttpResponse response;
    try {
      response = request.execute();
    } catch (HttpResponseException e) {
      if (authFromCookie && e.getStatusCode() == 401) {
        checkStateNotNull(COOKIE_MANAGER_MAP.get(cookieManagerKey)).getCookieStore().removeAll();
        // execute again without cookies to refresh the token.
        return execute(request);
      } else { // we might need to handle other response codes here.
        throw e;
      }
    }

    storeCookiesInCookieManager(response.getHeaders());
    return response;
  }

  private void setCookiesFromCookieManager(HttpHeaders httpHeaders) {
    httpHeaders.setCookie(
        checkStateNotNull(COOKIE_MANAGER_MAP.get(cookieManagerKey)).getCookieStore().getCookies()
            .stream()
            .map(s -> s.getName() + "=" + s.getValue())
            .collect(Collectors.joining(";")));
  }

  private void storeCookiesInCookieManager(HttpHeaders headers) {
    List<String> cookiesHeader = headers.getHeaderStringValues(COOKIES_HEADER);
    if (cookiesHeader != null) {
      for (String cookie : cookiesHeader) {
        checkStateNotNull(COOKIE_MANAGER_MAP.get(cookieManagerKey))
            .getCookieStore()
            .add(null, HttpCookie.parse(cookie).get(0));
      }
    }
  }

  private static String urlEncode(String queueName) throws UnsupportedEncodingException {
    return URLEncoder.encode(queueName, StandardCharsets.UTF_8.name());
  }

  private <T> T mapJsonToClass(String content, Class<T> mapSuccessToClass)
      throws JsonProcessingException {
    return objectMapper.readValue(content, mapSuccessToClass);
  }

  public long getBacklogBytes(String queueName) throws IOException {
    BrokerResponse response = getQueueResponse(queueName);
    if (response.content == null) {
      throw new IOException("SolaceIO: response from SEMP is empty!");
    }
    Queue q = mapJsonToClass(response.content, Queue.class);
    return q.data().msgSpoolUsage();
  }

  private static class CookieManagerKey implements Serializable {
    private final String baseUrl;
    private final String username;

    CookieManagerKey(String baseUrl, String username) {
      this.baseUrl = baseUrl;
      this.username = username;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CookieManagerKey)) {
        return false;
      }
      CookieManagerKey that = (CookieManagerKey) o;
      return Objects.equals(baseUrl, that.baseUrl) && Objects.equals(username, that.username);
    }

    @Override
    public int hashCode() {
      return Objects.hash(baseUrl, username);
    }
  }
}
