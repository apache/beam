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

import com.google.protobuf.Message;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link GoogleAdsIO} provides an API for reading from the <a
 * href="https://developers.google.com/google-ads/api/docs/start">Google Ads API</a> over supported
 * versions of the Google Ads client libraries.
 *
 * @see GoogleAdsV19
 */
public abstract class GoogleAdsIO<GoogleAdsRowT, SearchGoogleAdsStreamRequestT> {

  @SuppressWarnings(
      "TypeParameterUnusedInFormals") // for source code backward compatible when underlying API
  // version changed
  public abstract <T extends PTransform<PCollection<String>, PCollection<GoogleAdsRowT>>> T read();

  @SuppressWarnings(
      "TypeParameterUnusedInFormals") // for source code backward compatible when underlying API
  // version changed
  public abstract <
          T extends
              PTransform<PCollection<SearchGoogleAdsStreamRequestT>, PCollection<GoogleAdsRowT>>>
      T readAll();

  public static GoogleAdsV19 current() {
    return GoogleAdsV19.INSTANCE;
  }

  /**
   * Implement this interface to create a {@link RateLimitPolicy}. This should be used to limit all
   * traffic sent to the Google Ads API for a pair of developer token and customer ID and any other
   * relevant attributes for the specific Google Ads API service being called.
   */
  public interface RateLimitPolicyFactory<GoogleAdsErrorT> extends Serializable {
    RateLimitPolicy<GoogleAdsErrorT> getRateLimitPolicy();
  }

  /**
   * This interface can be used to implement custom client-side rate limiting policies. Custom
   * policies should follow best practices for interacting with the Google Ads API.
   *
   * @see <a href="https://developers.google.com/google-ads/api/docs/best-practices/overview">Best
   *     Practices in the Google Ads documentation</a>
   */
  public interface RateLimitPolicy<GoogleAdsErrorT> {
    /**
     * Called before a request is sent.
     *
     * @param developerToken The developer token used for the request.
     * @param customerId The customer ID specified on the request.
     * @param request Any Google Ads API request.
     * @throws InterruptedException
     */
    void onBeforeRequest(@Nullable String developerToken, String customerId, Message request)
        throws InterruptedException;

    /**
     * Called after a request succeeds.
     *
     * @param developerToken The developer token used for the request.
     * @param customerId The customer ID specified on the request.
     * @param request Any Google Ads API request.
     */
    void onSuccess(@Nullable String developerToken, String customerId, Message request);

    /**
     * Called after a request fails with a retryable error.
     *
     * @param developerToken The developer token used for the request.
     * @param customerId The customer ID specified on the request.
     * @param request Any Google Ads API request.
     * @param error A retryable error.
     */
    void onError(
        @Nullable String developerToken, String customerId, Message request, GoogleAdsErrorT error);
  }
}
