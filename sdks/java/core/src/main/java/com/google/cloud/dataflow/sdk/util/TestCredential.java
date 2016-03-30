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
package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.testing.http.MockHttpTransport;

import java.io.IOException;

/**
 * Fake credential, for use in testing.
 */
public class TestCredential extends Credential {

  private final String token;

  public TestCredential() {
    this("NULL");
  }

  public TestCredential(String token) {
    super(new Builder(
        BearerToken.authorizationHeaderAccessMethod())
        .setTransport(new MockHttpTransport()));
    this.token = token;
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    TokenResponse response = new TokenResponse();
    response.setExpiresInSeconds(5L * 60);
    response.setAccessToken(token);
    return response;
  }
}
