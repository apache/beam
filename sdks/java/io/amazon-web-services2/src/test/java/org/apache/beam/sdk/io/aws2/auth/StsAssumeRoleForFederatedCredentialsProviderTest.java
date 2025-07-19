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
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.aws2.auth.StsAssumeRoleForFederatedCredentialsProvider.CredentialsProviderDelegate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

@RunWith(MockitoJUnitRunner.class)
public class StsAssumeRoleForFederatedCredentialsProviderTest {
  private static final String AUDIENCE = "some static audience";
  private static final String ASSUMED_ROLE = "some role";
  private static final String TEST_WEBTOKEN_PROVIDER =
      "org.apache.beam.sdk.io.aws2.auth.StsAssumeRoleForFederatedCredentialsProviderTest$TestTokenProvider";
  private static final String FAKE_ACCESS_KEY = "some-access-key";
  private static final String FAKE_SECRET_KEY = "some-secret-key";

  @Mock private AwsCredentials mockedCredentials;
  @Mock private CredentialsProviderDelegate mockedProvider;

  @Before
  public void before() {
    when(mockedCredentials.accessKeyId()).thenReturn(FAKE_ACCESS_KEY);
    when(mockedCredentials.secretAccessKey()).thenReturn(FAKE_SECRET_KEY);
    when(mockedProvider.resolveCredentials()).thenReturn(mockedCredentials);
  }

  @Test
  public void retrieveAwsCredentials() {
    StsAssumeRoleForFederatedCredentialsProvider provider =
        StsAssumeRoleForFederatedCredentialsProvider.builder()
            .setAssumedRoleArn(ASSUMED_ROLE)
            .setAudience(AUDIENCE)
            .setWebIdTokenProviderFQCN(TEST_WEBTOKEN_PROVIDER)
            .setCredentialsProviderDelegate(mockedProvider)
            .build();

    AwsCredentials credentials = provider.resolveCredentials();

    // make sure we are using the faked credentials, not something set on a local profile.
    assertThat(credentials.accessKeyId()).isEqualTo(FAKE_ACCESS_KEY);
    assertThat(credentials.secretAccessKey()).isEqualTo(FAKE_SECRET_KEY);
  }

  @Test(expected = IllegalStateException.class)
  public void mustFailWithoutRole() {
    StsAssumeRoleForFederatedCredentialsProvider.builder()
        .setAudience(AUDIENCE)
        .setWebIdTokenProviderFQCN(TEST_WEBTOKEN_PROVIDER)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void mustFailWithoutAudience() {
    StsAssumeRoleForFederatedCredentialsProvider.builder()
        .setAssumedRoleArn(ASSUMED_ROLE)
        .setWebIdTokenProviderFQCN(TEST_WEBTOKEN_PROVIDER)
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void mustFailWithoutIdTokenProvider() {
    StsAssumeRoleForFederatedCredentialsProvider.builder()
        .setAssumedRoleArn(ASSUMED_ROLE)
        .setAudience(AUDIENCE)
        .build();
  }

  public static class TestTokenProvider implements WebIdTokenProvider {
    @Override
    public String resolveTokenValue(String audience) {
      return "some token for audience " + audience;
    }
  }
}
