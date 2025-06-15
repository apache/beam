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
package org.apache.beam.sdk.io.aws2.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.AccessToken;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.IdTokenCredentials;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleADCIdTokenProviderTest {

  @Mock private IdTokenCredentials idTokenCredentials;
  @Mock private AccessToken accessToken;

  @Before
  public void before() throws IOException {
    String header = "{\"alg\": \"RS256\",\"typ\": \"JWT\"}";
    String payload =
        "{\"aud\": \"some-audience\","
            + "\"azp\": \"some-email@google.com\","
            + "\"email\": \"some-email@google.com\","
            + "\"email_verified\": true,"
            + "\"iss\": \"https://accounts.google.com\"}";
    String signature = "some-garbled-data-to-be-encoded";
    String returnedToken =
        Base64.getUrlEncoder().encodeToString(header.getBytes(StandardCharsets.UTF_8))
            + "."
            + Base64.getUrlEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8))
            + "."
            + Base64.getUrlEncoder().encodeToString(signature.getBytes(StandardCharsets.UTF_8));
    when(accessToken.getTokenValue()).thenReturn(returnedToken);
    when(idTokenCredentials.refreshAccessToken()).thenReturn(accessToken);
  }

  @Test
  public void retrieveTokenValueWithAudience() {
    GoogleADCIdTokenProvider provider = spy(new GoogleADCIdTokenProvider());
    when(provider.createIdTokenWithApplicationDefaultCredentials(anyString()))
        .thenReturn(idTokenCredentials);
    String audience = "some-audience";

    String token = provider.resolveTokenValue(audience);

    String decodedToken =
        new String(
            Base64.getUrlDecoder().decode(Iterables.get(Splitter.on('.').split(token), 1)),
            StandardCharsets.UTF_8);
    assertThat(decodedToken).contains(audience);
  }
}
