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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import java.io.IOException;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SempBasicAuthClientExecutorTest {

  @Test
  public void testExecuteStatus4xx() {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(404);
                response.setContentType(Json.MEDIA_TYPE);
                response.setContent(
                    "{\"meta\":{\"error\":{\"code\":404,\"description\":\"some"
                        + " error\",\"status\":\"xx\"}}}");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName", requestFactory);

    assertThrows(HttpResponseException.class, () -> client.getQueueResponse("queue"));
  }

  @Test
  public void testExecuteStatus3xx() {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(301);
                response.setContentType(Json.MEDIA_TYPE);
                response.setContent(
                    "{\"meta\":{\"error\":{\"code\":301,\"description\":\"some"
                        + " error\",\"status\":\"xx\"}}}");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName", requestFactory);

    assertThrows(HttpResponseException.class, () -> client.getQueueResponse("queue"));
  }

  /**
   * In this test case, we test a situation when a session that we used to authenticate to Semp
   * expires.
   *
   * <p>To test this scenario, we need to do the following:
   *
   * <ol>
   *   <li>Send the first request, to initialize a session. This request has to contain the Basic
   *       Auth header and should not include any cookie headers. The response for this request
   *       contains a session cookie we can re-use in the following requests.
   *   <li>Send the second request - this request should use a cookie from the previous response.
   *       There should be no Authorization header. To simulate an expired session scenario, we set
   *       the response of this request to the "401 Unauthorized". This should cause a the request
   *       to be retried, this time with the Authorization header.
   *   <li>Validate the third request to contain the Basic Auth header and no session cookies.
   * </ol>
   */
  @Test
  public void testExecuteWithUnauthorized() throws IOException {
    // Making it a final array, so that we can reference it from within the MockHttpTransport
    // instance
    final int[] requestCounter = {0};
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                if (requestCounter[0] == 0) {
                  // The first request has to include Basic Auth header
                  assertTrue(this.getHeaders().containsKey("authorization"));
                  List<String> authorizationHeaders = this.getHeaders().get("authorization");
                  assertEquals(1, authorizationHeaders.size());
                  assertTrue(authorizationHeaders.get(0).contains("Basic"));
                  assertFalse(this.getHeaders().containsKey("cookie"));

                  // Set the response to include Session cookies
                  response
                      .setHeaderNames(ImmutableList.of("Set-Cookie", "Set-Cookie"))
                      .setHeaderValues(
                          ImmutableList.of(
                              "ProxySession=JddSdJaGo6FYYmQk6nt8jXxFtq6n3FCFR14ebzRGQ5w;"
                                  + " HttpOnly; SameSite=Strict;"
                                  + " Path=/proxy; Max-Age=2592000",
                              "Session=JddSdJaGo6FYYmQk6nt8jXxFtq6n3FCFR14ebzRGQ5w;"
                                  + " HttpOnly; SameSite=Strict;"
                                  + " Path=/SEMP; Max-Age=2592000"));
                  response.setStatusCode(200);
                } else if (requestCounter[0] == 1) {
                  // The second request does not include Basic Auth header
                  assertFalse(this.getHeaders().containsKey("authorization"));
                  // It must include a cookie header
                  assertTrue(this.getHeaders().containsKey("cookie"));
                  boolean hasSessionCookie =
                      this.getHeaders().get("cookie").stream()
                              .filter(
                                  c ->
                                      c.contains(
                                          "Session=JddSdJaGo6FYYmQk6nt8jXxFtq6n3FCFR14ebzRGQ5w"))
                              .count()
                          == 1;
                  assertTrue(hasSessionCookie);

                  // Let's assume the Session expired - we return the 401
                  // unauthorized
                  response.setStatusCode(401);
                } else {
                  // The second request has to be retried with a Basic Auth header
                  // this time
                  assertTrue(this.getHeaders().containsKey("authorization"));
                  List<String> authorizationHeaders = this.getHeaders().get("authorization");
                  assertEquals(1, authorizationHeaders.size());
                  assertTrue(authorizationHeaders.get(0).contains("Basic"));
                  assertFalse(this.getHeaders().containsKey("cookie"));

                  response.setStatusCode(200);
                }
                response.setContentType(Json.MEDIA_TYPE);
                requestCounter[0]++;
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName", requestFactory);

    // The first, initial request
    client.getQueueResponse("queue");
    // The second request, which will try to authenticate with a cookie, and then with Basic
    // Auth when it receives a 401 unauthorized
    client.getQueueResponse("queue");

    // There should be 3 requests executed:
    // the first one is the initial one with Basic Auth,
    // the second one uses the session cookie, but we simulate it being expired,
    // so there should be a third request with Basic Auth to create a new session.
    assertEquals(3, requestCounter[0]);
  }

  @Test
  public void testGetQueueResponseEncoding() throws IOException {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                assertTrue(url.contains("queues/queue%2Fxxx%2Fyyy"));
                assertTrue(url.contains("msgVpns/vpnName%232"));
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName#2", requestFactory);

    client.getQueueResponse("queue/xxx/yyy");
  }

  @Test
  public void testCreateQueueResponseEncoding() throws IOException {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                assertTrue(this.getContentAsString().contains("\"queueName\":\"queue/xxx/yyy\""));
                assertTrue(url.contains("msgVpns/vpnName%232"));
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName#2", requestFactory);

    client.createQueueResponse("queue/xxx/yyy");
  }

  @Test
  public void testCreateSubscriptionResponseEncoding() throws IOException {
    MockHttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() throws IOException {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                assertTrue(this.getContentAsString().contains("\"queueName\":\"queue/xxx/yyy\""));
                assertTrue(
                    this.getContentAsString().contains("\"subscriptionTopic\":\"topic/aaa\""));
                assertTrue(url.contains("queues/queue%2Fxxx%2Fyyy/subscriptions"));
                assertTrue(url.contains("msgVpns/vpnName%232"));
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory();
    SempBasicAuthClientExecutor client =
        new SempBasicAuthClientExecutor(
            "http://host", "username", "password", "vpnName#2", requestFactory);

    client.createSubscriptionResponse("queue/xxx/yyy", "topic/aaa");
  }
}
