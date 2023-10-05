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

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.auth.Credentials;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The default way to construct a {@link GoogleAdsClient}. */
public class DefaultGoogleAdsClientFactory implements GoogleAdsClientFactory {
  private static final DefaultGoogleAdsClientFactory INSTANCE = new DefaultGoogleAdsClientFactory();

  public static DefaultGoogleAdsClientFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public GoogleAdsClient newGoogleAdsClient(
      GoogleAdsOptions options,
      @Nullable String developerToken,
      @Nullable Long linkedCustomerId,
      @Nullable Long loginCustomerId) {

    GoogleAdsClient.Builder builder = GoogleAdsClient.newBuilder();

    Credentials credentials = options.getGoogleAdsCredential();
    if (credentials != null) {
      builder.setCredentials(credentials);
    }

    if (options.getGoogleAdsEndpoint() != null) {
      builder.setEndpoint(options.getGoogleAdsEndpoint());
    }

    String developerTokenFromOptions = options.getGoogleAdsDeveloperToken();
    if (developerToken != null) {
      builder.setDeveloperToken(developerToken);
    } else if (developerTokenFromOptions != null) {
      builder.setDeveloperToken(developerTokenFromOptions);
    }

    if (linkedCustomerId != null) {
      builder.setLinkedCustomerId(linkedCustomerId);
    }

    if (loginCustomerId != null) {
      builder.setLoginCustomerId(loginCustomerId);
    }

    return builder.build();
  }
}
