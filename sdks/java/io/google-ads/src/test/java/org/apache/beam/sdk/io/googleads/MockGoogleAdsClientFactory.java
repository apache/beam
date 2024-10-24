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

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v17.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v17.services.stub.GoogleAdsServiceStub;
import org.checkerframework.checker.nullness.qual.Nullable;

class MockGoogleAdsClientFactory implements GoogleAdsClientFactory {
  static final GoogleAdsServiceStub GOOGLE_ADS_SERVICE_STUB_V17 =
      mock(GoogleAdsServiceStub.class, withSettings().defaultAnswer(RETURNS_DEEP_STUBS));

  @Override
  public GoogleAdsClient newGoogleAdsClient(
      GoogleAdsOptions options,
      @Nullable String developerToken,
      @Nullable Long linkedCustomerId,
      @Nullable Long loginCustomerId) {
    GoogleAdsClient mockGoogleAdsClient =
        mock(GoogleAdsClient.class, withSettings().defaultAnswer(RETURNS_DEEP_STUBS));
    when(mockGoogleAdsClient.getVersion17().createGoogleAdsServiceClient())
        .thenReturn(GoogleAdsServiceClient.create(GOOGLE_ADS_SERVICE_STUB_V17));
    return mockGoogleAdsClient;
  }
}
