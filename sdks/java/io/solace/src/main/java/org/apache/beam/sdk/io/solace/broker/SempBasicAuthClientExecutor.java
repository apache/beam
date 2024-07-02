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
import java.net.CookieManager;
import java.net.HttpCookie;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * A class to execute requests to SEMP v2 with Basic Auth authentication.
 *
 * <p>This approach takes advantage of <a
 * href="https://docs.solace.com/Admin/SEMP/SEMP-Security.htm#Sessions">SEMP Sessions</a>. The
 * session is established when a user authenticates with HTTP Basic authentication. When the
 * response is 401 Unauthorized, the client will execute an additional request with Basic Auth
 * header to refresh the token.
 */
class SempBasicAuthClientExecutor implements Serializable {
  private static final CookieManager COOKIE_MANAGER = new CookieManager();
  private static final String COOKIES_HEADER = "Set-Cookie";

  private final String username;
  private final String messageVpn;
  private final String baseUrl;
  private final String password;
  private final transient HttpRequestFactory requestFactory;

  SempBasicAuthClientExecutor(
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
  }

  private static String getQueueEndpoint(String messageVpn, String queueName) {
    return String.format("/monitor/msgVpns/%s/queues/%s", messageVpn, queueName);
  }

  private static String createQueueEndpoint(String messageVpn) {
    return String.format("/config/msgVpns/%s/queues", messageVpn);
  }

  private static String subscriptionEndpoint(String messageVpn, String queueName) {
    return String.format("/config/msgVpns/%s/queues/%s/subscriptions", messageVpn, queueName);
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
    request.setNumberOfRetries(2);
    HttpHeaders httpHeaders = new HttpHeaders();
    boolean authFromCookie = COOKIE_MANAGER.getCookieStore().getCookies().size() > 0;
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
        COOKIE_MANAGER.getCookieStore().removeAll();
        // execute again without cookies to refresh the token.
        return execute(request);
      } else {
        throw e;
      }
    }

    storeCookiesInCookieManager(response.getHeaders());
    return response;
  }

  private void setCookiesFromCookieManager(HttpHeaders httpHeaders) {
    httpHeaders.setCookie(
        COOKIE_MANAGER.getCookieStore().getCookies().stream()
            .map(s -> s.getName() + "=" + s.getValue())
            .collect(Collectors.joining(";")));
  }

  private void storeCookiesInCookieManager(HttpHeaders headers) {
    List<String> cookiesHeader = headers.getHeaderStringValues(COOKIES_HEADER);
    if (cookiesHeader != null) {
      for (String cookie : cookiesHeader) {
        COOKIE_MANAGER.getCookieStore().add(null, HttpCookie.parse(cookie).get(0));
      }
    }
  }
}
