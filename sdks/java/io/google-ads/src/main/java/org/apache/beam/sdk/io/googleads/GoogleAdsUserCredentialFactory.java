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
package org.apache.beam.sdk.io.googleads;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.auth.oauth2.UserCredentials;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Constructs and returns {@link Credentials} to be used by Google Ads API calls. This factory only
 * supports {@link com.google.auth.oauth2.UserCredentials}, {@link
 * com.google.auth.oauth2.ServiceAccountCredentials} and domain-wide delegation are not supported.
 */
public class GoogleAdsUserCredentialFactory implements CredentialFactory {
  // The OAuth client ID, client secret, and refresh token.
  private String clientId;
  private String clientSecret;
  private String refreshToken;

  private GoogleAdsUserCredentialFactory(
      String clientId, String clientSecret, String refreshToken) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = refreshToken;
  }

  public static GoogleAdsUserCredentialFactory fromOptions(PipelineOptions options) {
    GoogleAdsOptions adsOptions = options.as(GoogleAdsOptions.class);

    checkArgument(
        adsOptions.getGoogleAdsClientId() != null
            && adsOptions.getGoogleAdsClientSecret() != null
            && adsOptions.getGoogleAdsRefreshToken() != null,
        "googleAdsClientId, googleAdsClientSecret and googleAdsRefreshToken must not be null");

    return new GoogleAdsUserCredentialFactory(
        adsOptions.getGoogleAdsClientId(),
        adsOptions.getGoogleAdsClientSecret(),
        adsOptions.getGoogleAdsRefreshToken());
  }

  /** Returns {@link Credentials} as configured by {@link GoogleAdsOptions}. */
  @Override
  public @Nullable Credentials getCredential() {
    return UserCredentials.newBuilder()
        .setClientId(clientId)
        .setClientSecret(clientSecret)
        .setRefreshToken(refreshToken)
        .build();
  }
}
