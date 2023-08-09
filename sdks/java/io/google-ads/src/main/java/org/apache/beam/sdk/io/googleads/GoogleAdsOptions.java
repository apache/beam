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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auth.Credentials;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options used to configure Google Ads API specific options. */
public interface GoogleAdsOptions extends PipelineOptions {
  /** Host endpoint to use for connections to the Google Ads API. */
  @Description("Host endpoint to use for connections to the Google Ads API.")
  @Default.String("googleads.googleapis.com:443")
  String getGoogleAdsEndpoint();

  void setGoogleAdsEndpoint(String endpoint);

  /**
   * OAuth 2.0 Client ID identifying the application.
   *
   * @see https://developers.google.com/google-ads/api/docs/oauth/overview
   * @see https://developers.google.com/identity/protocols/oauth2
   */
  @Description("OAuth 2.0 Client ID identifying the application.")
  String getGoogleAdsClientId();

  void setGoogleAdsClientId(String clientId);

  /**
   * OAuth 2.0 Client Secret for the specified Client ID.
   *
   * @see https://developers.google.com/google-ads/api/docs/oauth/overview
   * @see https://developers.google.com/identity/protocols/oauth2
   */
  @Description("OAuth 2.0 Client Secret for the specified Client ID.")
  String getGoogleAdsClientSecret();

  void setGoogleAdsClientSecret(String clientSecret);

  /**
   * OAuth 2.0 Refresh Token for the user connecting to the Google Ads API.
   *
   * @see https://developers.google.com/google-ads/api/docs/oauth/overview
   * @see https://developers.google.com/identity/protocols/oauth2
   */
  @Description("OAuth 2.0 Refresh Token for the user connecting to the Google Ads API.")
  String getGoogleAdsRefreshToken();

  void setGoogleAdsRefreshToken(String refreshToken);

  /** Google Ads developer token for the user connecting to the Google Ads API. */
  @Description("Google Ads developer token for the user connecting to the Google Ads API.")
  @Nullable
  String getGoogleAdsDeveloperToken();

  void setGoogleAdsDeveloperToken(String developerToken);

  /**
   * The class of the credential factory to create credentials if none have been explicitly set.
   *
   * @see #getGoogleAdsCredential()
   */
  @Description(
      "The class of the credential factory to create credentials if none have been explicitly set.")
  @Default.Class(GoogleAdsUserCredentialFactory.class)
  Class<? extends CredentialFactory> getGoogleAdsCredentialFactoryClass();

  void setGoogleAdsCredentialFactoryClass(
      Class<? extends CredentialFactory> credentialFactoryClass);

  /**
   * The credential instance that should be used to authenticate against the Google Ads API.
   * Defaults to a credential instance constructed by the credential factory.
   *
   * @see #getGoogleAdsCredential()
   * @see https://github.com/googleapis/google-auth-library-java
   */
  @JsonIgnore
  @Description(
      "The credential instance that should be used to authenticate against the Google Ads API. "
          + "Defaults to a credential instance constructed by the credential factory.")
  @Default.InstanceFactory(GoogleAdsCredentialsFactory.class)
  @Nullable
  Credentials getGoogleAdsCredential();

  void setGoogleAdsCredential(Credentials credential);

  /**
   * Attempts to load the Google Ads credentials. See {@link CredentialFactory#getCredential()} for
   * more details.
   */
  class GoogleAdsCredentialsFactory implements DefaultValueFactory<@Nullable Credentials> {
    @Override
    public @Nullable Credentials create(PipelineOptions options) {
      GoogleAdsOptions googleAdsOptions = options.as(GoogleAdsOptions.class);
      try {
        CredentialFactory factory =
            InstanceBuilder.ofType(CredentialFactory.class)
                .fromClass(googleAdsOptions.getGoogleAdsCredentialFactoryClass())
                .fromFactoryMethod("fromOptions")
                .withArg(PipelineOptions.class, options)
                .build();
        return factory.getCredential();
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException("Unable to obtain credential", e);
      }
    }
  }
}
