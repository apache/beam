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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v17.errors.GoogleAdsError;
import com.google.ads.googleads.v17.errors.GoogleAdsException;
import com.google.ads.googleads.v17.errors.GoogleAdsFailure;
import com.google.ads.googleads.v17.errors.InternalErrorEnum;
import com.google.ads.googleads.v17.errors.QuotaErrorEnum;
import com.google.ads.googleads.v17.services.GoogleAdsRow;
import com.google.ads.googleads.v17.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v17.services.SearchGoogleAdsStreamRequest;
import com.google.ads.googleads.v17.services.SearchGoogleAdsStreamResponse;
import com.google.auto.value.AutoValue;
import com.google.protobuf.Message;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.RateLimiter;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.joda.time.Duration;

/**
 * {@link GoogleAdsV17} provides an API to read Google Ads API v17 reports.
 *
 * <p>The Google Ads API does not use service account credentials in the same way as Google Cloud
 * Platform APIs do. Service account credentials are typically only used to delegate (using
 * domain-wide delegation) access through end user accounts. Providing credentials using the OAuth2
 * desktop flow may be preferable over domain wide delegation. Please refer to the <a
 * href="https://developers.google.com/google-ads/api/docs/oauth/overview">Google Ads API
 * documentation</a> for more information on OAuth2 in the Google Ads API.
 *
 * <p>Defaults for OAuth 2.0 credentials, refresh token and developer token can be provided using
 * the following flags:
 *
 * <pre>
 *   --googleAdsClientId=your-client-id
 *   --googleAdsClientSecret=your-client-secret
 *   --googleAdsRefreshToken=your-refresh-token
 *   --googleAdsDeveloperToken=your-developer-token
 * </pre>
 *
 * <p>Use {@link GoogleAdsV17#read()} to read either a bounded or unbounded {@link PCollection} of
 * {@link GoogleAdsRow} from a single <a
 * href="https://developers.google.com/google-ads/api/docs/query/overview">Google Ads Query
 * Language</a> query using {@link Read#withQuery(String)} and a {@link PCollection} of customer
 * IDs. Alternatively, use {@link GoogleAdsV17#readAll()} to read either a bounded or unbounded
 * {@link PCollection} of {@link GoogleAdsRow} from a {@link PCollection} of {@link
 * SearchGoogleAdsStreamRequest} potentially containing many different queries.
 *
 * <p>For example, using {@link GoogleAdsV17#read()}:
 *
 * <pre>{@code
 * Pipeline p = Pipeline.create();
 * PCollection<String> customerIds =
 *     p.apply(Create.of(Long.toString(1234567890L)));
 * PCollection<GoogleAdsRow> rows =
 *     customerIds.apply(
 *         GoogleAdsIO.v17()
 *             .read()
 *             .withRateLimitPolicy(MY_RATE_LIMIT_POLICY)
 *             .withQuery(
 *                 "SELECT"
 *                     + "campaign.id,"
 *                     + "campaign.name,"
 *                     + "campaign.status"
 *                     + "FROM campaign"));
 * p.run();
 * }</pre>
 *
 * <p>Alternatively, using {@link GoogleAdsV17#readAll()} to execute requests from a {@link
 * PCollection} of {@link SearchGoogleAdsStreamRequest}:
 *
 * <pre>{@code
 * Pipeline p = Pipeline.create();
 * PCollection<SearchGoogleAdsStreamRequest> requests =
 *     p.apply(
 *         Create.of(
 *             ImmutableList.of(
 *                 SearchGoogleAdsStreamRequest.newBuilder()
 *                     .setCustomerId(Long.toString(1234567890L))
 *                     .setQuery(
 *                         "SELECT"
 *                             + "campaign.id,"
 *                             + "campaign.name,"
 *                             + "campaign.status"
 *                             + "FROM campaign")
 *                     .build())));
 * PCollection<GoogleAdsRow> rows =
 *     requests.apply(GoogleAdsIO.v17().readAll().withRateLimitPolicy(MY_RATE_LIMIT_POLICY));
 * p.run();
 * }</pre>
 *
 * <h2>Client-side rate limiting</h2>
 *
 * On construction of a {@link GoogleAdsV17#read()} or {@link GoogleAdsV17#readAll()} transform a
 * rate limiting policy must be specified to stay well under the assigned quota for the Google Ads
 * API. The Google Ads API enforces global rate limits from the developer token down to the customer
 * ID and depending on the access level of the developer token a limit on the total number of
 * executed operations per day. See <a
 * href="https://developers.google.com/google-ads/api/docs/best-practices/rate-limits">Rate
 * Limits</a> and <a
 * href="https://developers.google.com/google-ads/api/docs/best-practices/quotas">API Limits and
 * Quotas</a> in the Google Ads documentation for more details.
 *
 * <p>It is recommended to host a shared rate limiting service to coordinate traffic to the Google
 * Ads API across all applications using the same developer token. Users of these transforms are
 * strongly advised to implement their own {@link RateLimitPolicy} and {@link
 * RateLimitPolicyFactory} to interact with a shared rate limiting service (e.g. <a
 * href="https://github.com/mailgun/gubernator">gubernator</a>) for any production workloads.
 *
 * <h2>Required Minimum Functionality</h2>
 *
 * Pipelines built using these transforms may still be subject to the Required Minimum Functionality
 * policy. Please review the policy carefully and have your tool reviewed by the Google Ads API
 * Review Team. See <a href="https://developers.google.com/google-ads/api/docs/rmf">Required Minimum
 * Functionality</a> and <a href="https://developers.google.com/google-ads/api/docs/rate-sheet">Rate
 * sheet & non-compliance fees</a> in the Google Ads API documentation for more details.
 *
 * @see GoogleAdsIO#v17()
 * @see GoogleAdsOptions
 * @see <a href="https://developers.google.com/google-ads/api/docs/best-practices/overview">Best
 *     Practices in the Google Ads documentation</a>
 */
public class GoogleAdsV17 {
  static final GoogleAdsV17 INSTANCE = new GoogleAdsV17();

  private GoogleAdsV17() {}

  public Read read() {
    return new AutoValue_GoogleAdsV17_Read.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .build();
  }

  public ReadAll readAll() {
    return new AutoValue_GoogleAdsV17_ReadAll.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .build();
  }

  /**
   * A {@link PTransform} that reads the results of a Google Ads query as {@link GoogleAdsRow}
   * objects.
   *
   * @see GoogleAdsIO#v17()
   * @see #readAll()
   */
  @AutoValue
  public abstract static class Read
      extends PTransform<PCollection<String>, PCollection<GoogleAdsRow>> {
    abstract @Nullable String getDeveloperToken();

    abstract @Nullable Long getLoginCustomerId();

    abstract @Nullable String getQuery();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract @Nullable RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDeveloperToken(@Nullable String developerToken);

      abstract Builder setLoginCustomerId(@Nullable Long loginCustomerId);

      abstract Builder setQuery(String query);

      abstract Builder setGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory);

      abstract Builder setRateLimitPolicyFactory(RateLimitPolicyFactory rateLimitPolicyFactory);

      abstract Read build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified developer token. A
     * developer token is required to access the Google Ads API.
     *
     * @param developerToken The developer token to set.
     * @return A new {@link Read} transform with the specified developer token.
     * @see GoogleAdsClient
     */
    public Read withDeveloperToken(@Nullable String developerToken) {
      return toBuilder().setDeveloperToken(developerToken).build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified login customer ID. A
     * login customer ID is only required for manager accounts.
     *
     * @param loginCustomerId The login customer ID to set.
     * @return A new {@link Read} transform with the specified login customer ID.
     * @see GoogleAdsClient
     */
    public Read withLoginCustomerId(@Nullable Long loginCustomerId) {
      return toBuilder().setLoginCustomerId(loginCustomerId).build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified query. The query will be
     * executed for each customer ID.
     *
     * @param query
     * @return A new {@link Read} transform with the specified query.
     * @see SearchGoogleAdsStreamRequest
     */
    public Read withQuery(String query) {
      checkArgumentNotNull(query, "query cannot be null");
      checkArgument(!query.isEmpty(), "query cannot be empty");

      return toBuilder().setQuery(query).build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified client factory. A {@link
     * GoogleAdsClientFactory} builds the {@link GoogleAdsClient} used to construct service clients.
     * The {@link DefaultGoogleAdsClientFactory} should be sufficient for most purposes unless the
     * construction of {@link GoogleAdsClient} requires customization.
     *
     * @param googleAdsClientFactory
     * @return A new {@link Read} transform with the specified client factory.
     * @see GoogleAdsClient
     */
    public Read withGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory) {
      checkArgumentNotNull(googleAdsClientFactory, "googleAdsClientFactory cannot be null");

      return toBuilder().setGoogleAdsClientFactory(googleAdsClientFactory).build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified rate limit policy
     * factory. A {@link RateLimitPolicyFactory} builds the {@link RateLimitPolicy} used to limit
     * the number of requests made by {@link ReadAll.ReadAllFn}. The Google Ads API enforces global
     * limits from the developer token down to the customer ID and it is recommended to host a
     * shared rate limiting service to coordinate traffic to the Google Ads API across all
     * applications using the same developer token. Users of these transforms are strongly advised
     * to implement their own {@link RateLimitPolicy} and {@link RateLimitPolicyFactory} to interact
     * with a shared rate limiting service for any production workloads.
     *
     * @param rateLimitPolicyFactory
     * @return A new {@link Read} transform with the specified rate limit policy factory.
     * @see GoogleAdsClient
     */
    public Read withRateLimitPolicy(RateLimitPolicyFactory rateLimitPolicyFactory) {
      checkArgumentNotNull(rateLimitPolicyFactory, "rateLimitPolicyFactory cannot be null");

      return toBuilder().setRateLimitPolicyFactory(rateLimitPolicyFactory).build();
    }

    @Override
    public PCollection<GoogleAdsRow> expand(PCollection<String> input) {
      String query = getQuery();
      RateLimitPolicyFactory rateLimitPolicyFactory = getRateLimitPolicyFactory();
      checkArgumentNotNull(query, "withQuery() is required");
      checkArgumentNotNull(rateLimitPolicyFactory, "withRateLimitPolicy() is required");

      return input
          .apply(
              MapElements.into(TypeDescriptor.of(SearchGoogleAdsStreamRequest.class))
                  .via(
                      customerId ->
                          SearchGoogleAdsStreamRequest.newBuilder()
                              .setCustomerId(customerId)
                              .setQuery(query)
                              .build()))
          .apply(
              INSTANCE
                  .readAll()
                  .withDeveloperToken(getDeveloperToken())
                  .withLoginCustomerId(getLoginCustomerId())
                  .withGoogleAdsClientFactory(getGoogleAdsClientFactory())
                  .withRateLimitPolicy(rateLimitPolicyFactory));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("query", String.valueOf(getQuery())).withLabel("Query"));
    }
  }

  /**
   * A {@link PTransform} that reads the results of many {@link SearchGoogleAdsStreamRequest}
   * objects as {@link GoogleAdsRow} objects. *
   *
   * @see GoogleAdsIO#v17()
   * @see #readAll()
   */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<SearchGoogleAdsStreamRequest>, PCollection<GoogleAdsRow>> {
    abstract @Nullable String getDeveloperToken();

    abstract @Nullable Long getLoginCustomerId();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract @Nullable RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDeveloperToken(@Nullable String developerToken);

      abstract Builder setLoginCustomerId(@Nullable Long loginCustomerId);

      abstract Builder setGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory);

      abstract Builder setRateLimitPolicyFactory(RateLimitPolicyFactory rateLimitPolicyFactory);

      abstract ReadAll build();
    }

    /**
     * Creates and returns a new {@link ReadAll} transform with the specified developer token. A
     * developer token is required to access the Google Ads API.
     *
     * @param developerToken The developer token to set.
     * @return A new {@link ReadAll} transform with the specified developer token.
     * @see GoogleAdsClient
     */
    public ReadAll withDeveloperToken(@Nullable String developerToken) {
      return toBuilder().setDeveloperToken(developerToken).build();
    }

    /**
     * Creates and returns a new {@link ReadAll} transform with the specified login customer ID. A
     * login customer ID is only required for manager accounts.
     *
     * @param loginCustomerId The login customer ID to set.
     * @return A new {@link ReadAll} transform with the specified login customer ID.
     * @see GoogleAdsClient
     */
    public ReadAll withLoginCustomerId(@Nullable Long loginCustomerId) {
      return toBuilder().setLoginCustomerId(loginCustomerId).build();
    }

    /**
     * Creates and returns a new {@link ReadAll} transform with the specified client factory. A
     * {@link GoogleAdsClientFactory} builds the {@link GoogleAdsClient} used to construct service
     * clients. The {@link DefaultGoogleAdsClientFactory} should be sufficient for most purposes
     * unless the construction of {@link GoogleAdsClient} requires customization.
     *
     * @param googleAdsClientFactory
     * @return A new {@link ReadAll} transform with the specified client factory.
     * @see GoogleAdsClient
     */
    public ReadAll withGoogleAdsClientFactory(GoogleAdsClientFactory googleAdsClientFactory) {
      checkArgumentNotNull(googleAdsClientFactory, "googleAdsClientFactory cannot be null");

      return toBuilder().setGoogleAdsClientFactory(googleAdsClientFactory).build();
    }

    /**
     * Creates and returns a new {@link ReadAll} transform with the specified rate limit policy
     * factory. A {@link RateLimitPolicyFactory} builds the {@link RateLimitPolicy} used to limit
     * the number of requests made by {@link ReadAll.ReadAllFn}. The Google Ads API enforces global
     * limits from the developer token down to the customer ID and it is recommended to host a
     * shared rate limiting service to coordinate traffic to the Google Ads API across all
     * applications using the same developer token. Users of these transforms are strongly advised
     * to implement their own {@link RateLimitPolicy} and {@link RateLimitPolicyFactory} to interact
     * with a shared rate limiting service for any production workloads.
     *
     * @param rateLimitPolicyFactory
     * @return A new {@link ReadAll} transform with the specified rate limit policy factory.
     * @see GoogleAdsClient
     */
    public ReadAll withRateLimitPolicy(RateLimitPolicyFactory rateLimitPolicyFactory) {
      checkArgumentNotNull(rateLimitPolicyFactory, "rateLimitPolicyFactory cannot be null");

      return toBuilder().setRateLimitPolicyFactory(rateLimitPolicyFactory).build();
    }

    @Override
    public PCollection<GoogleAdsRow> expand(PCollection<SearchGoogleAdsStreamRequest> input) {
      GoogleAdsOptions options = input.getPipeline().getOptions().as(GoogleAdsOptions.class);

      checkArgument(
          options.getGoogleAdsDeveloperToken() != null || getDeveloperToken() != null,
          "either --googleAdsDeveloperToken or .withDeveloperToken() is required");
      checkArgumentNotNull(getRateLimitPolicyFactory(), "withRateLimitPolicy() is required");

      return input.apply(ParDo.of(new ReadAllFn(this)));
    }

    /**
     * A {@link DoFn} that reads reports from Google Ads for each query using the {@code
     * SearchStream} method.
     */
    @VisibleForTesting
    static class ReadAllFn extends DoFn<SearchGoogleAdsStreamRequest, GoogleAdsRow> {
      // The default retry configuration is based on that of services with a comparable
      // potential volume of requests to the Google Ads API.
      private static final int MAX_RETRIES = 5;
      private static final FluentBackoff BACKOFF =
          FluentBackoff.DEFAULT
              .withExponent(2.0)
              .withInitialBackoff(Duration.standardSeconds(30))
              .withMaxRetries(MAX_RETRIES);

      @VisibleForTesting static Sleeper sleeper = Sleeper.DEFAULT;

      private final GoogleAdsV17.ReadAll spec;

      private transient @Nullable GoogleAdsClient googleAdsClient;
      private transient @Nullable GoogleAdsServiceClient googleAdsServiceClient;
      private transient @Nullable RateLimitPolicy rateLimitPolicy;

      ReadAllFn(GoogleAdsV17.ReadAll spec) {
        this.spec = spec;
      }

      @Setup
      @EnsuresNonNull({"googleAdsClient", "googleAdsServiceClient", "rateLimitPolicy"})
      public void setup(PipelineOptions options) {
        GoogleAdsOptions adsOptions = options.as(GoogleAdsOptions.class);

        final GoogleAdsClient googleAdsClient =
            spec.getGoogleAdsClientFactory()
                .newGoogleAdsClient(
                    adsOptions, spec.getDeveloperToken(), null, spec.getLoginCustomerId());
        final GoogleAdsServiceClient googleAdsServiceClient =
            googleAdsClient.getVersion17().createGoogleAdsServiceClient();
        final RateLimitPolicy rateLimitPolicy =
            checkStateNotNull(spec.getRateLimitPolicyFactory()).getRateLimitPolicy();

        this.googleAdsClient = googleAdsClient;
        this.googleAdsServiceClient = googleAdsServiceClient;
        this.rateLimitPolicy = rateLimitPolicy;
      }

      @ProcessElement
      @RequiresNonNull({"googleAdsClient", "googleAdsServiceClient", "rateLimitPolicy"})
      public void processElement(ProcessContext c) throws IOException, InterruptedException {
        final GoogleAdsClient googleAdsClient = this.googleAdsClient;
        final GoogleAdsServiceClient googleAdsServiceClient = this.googleAdsServiceClient;
        final RateLimitPolicy rateLimitPolicy = this.rateLimitPolicy;

        BackOff backoff = BACKOFF.backoff();
        BackOff nextBackoff = backoff;
        GoogleAdsException lastException = null;

        SearchGoogleAdsStreamRequest request = c.element();
        String token = googleAdsClient.getDeveloperToken();
        String customerId = request.getCustomerId();

        do {
          rateLimitPolicy.onBeforeRequest(token, customerId, request);

          try {
            for (SearchGoogleAdsStreamResponse response :
                googleAdsServiceClient.searchStreamCallable().call(request)) {
              for (GoogleAdsRow row : response.getResultsList()) {
                c.output(row);
              }
            }
            rateLimitPolicy.onSuccess(token, customerId, request);
            return;
          } catch (GoogleAdsException e) {
            GoogleAdsError retryableError =
                findFirstRetryableError(e.getGoogleAdsFailure())
                    .orElseThrow(() -> new IOException(e));

            rateLimitPolicy.onError(token, customerId, request, retryableError);

            // If the error happens to carry a suggested retry delay, then use that instead.
            // Retry these errors without incrementing the retry count or backoff interval.
            // For all other retryable errors fall back to the existing backoff.
            if (retryableError.getDetails().getQuotaErrorDetails().hasRetryDelay()) {
              nextBackoff =
                  new BackOff() {
                    @Override
                    public void reset() {}

                    @Override
                    public long nextBackOffMillis() {
                      return Durations.toMillis(
                          retryableError.getDetails().getQuotaErrorDetails().getRetryDelay());
                    }
                  };
            } else {
              nextBackoff = backoff;
            }
          }
        } while (BackOffUtils.next(sleeper, nextBackoff));

        throw new IOException(
            String.format(
                "Unable to get Google Ads response after retrying %d times using query (%s)",
                MAX_RETRIES, request.getQuery()),
            lastException);
      }

      @Teardown
      public void teardown() {
        if (googleAdsServiceClient != null) {
          googleAdsServiceClient.close();
        }
      }

      private Optional<GoogleAdsError> findFirstRetryableError(GoogleAdsFailure e) {
        return e.getErrorsList().stream()
            .filter(
                err ->
                    // Unexpected internal error
                    err.getErrorCode().getInternalError()
                            == InternalErrorEnum.InternalError.INTERNAL_ERROR
                        ||
                        // Unexpected transient error
                        err.getErrorCode().getInternalError()
                            == InternalErrorEnum.InternalError.TRANSIENT_ERROR
                        ||
                        // Too many requests
                        err.getErrorCode().getQuotaError()
                            == QuotaErrorEnum.QuotaError.RESOURCE_EXHAUSTED
                        ||
                        // Too many requests in a short amount of time
                        err.getErrorCode().getQuotaError()
                            == QuotaErrorEnum.QuotaError.RESOURCE_TEMPORARILY_EXHAUSTED)
            .findFirst();
      }
    }
  }

  /**
   * Implement this interface to create a {@link RateLimitPolicy}. This should be used to limit all
   * traffic sent to the Google Ads API for a pair of developer token and customer ID and any other
   * relevant attributes for the specific Google Ads API service being called.
   */
  public interface RateLimitPolicyFactory extends Serializable {
    RateLimitPolicy getRateLimitPolicy();
  }

  /**
   * This interface can be used to implement custom client-side rate limiting policies. Custom
   * policies should follow best practices for interacting with the Google Ads API.
   *
   * @see <a href="https://developers.google.com/google-ads/api/docs/best-practices/overview">Best
   *     Practices in the Google Ads documentation</a>
   */
  public interface RateLimitPolicy {
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
        @Nullable String developerToken, String customerId, Message request, GoogleAdsError error);
  }

  /**
   * This rate limit policy wraps a {@link RateLimiter} and can be used in low volume and
   * development use cases as a client-side rate limiting policy. This policy does not enforce a
   * global (per pipeline or otherwise) rate limit to requests and should not be used in deployments
   * where the Google Ads API quota is shared between multiple applications.
   *
   * <p>This policy can be used to limit requests across all {@link GoogleAdsV17.Read} or {@link
   * GoogleAdsV17.ReadAll} transforms by defining and using a {@link
   * GoogleAdsV17.RateLimitPolicyFactory} which holds a shared static {@link
   * GoogleAdsV17.SimpleRateLimitPolicy}. Note that the desired rate must be divided by the expected
   * maximum number of workers for the pipeline, otherwise the pipeline may exceed the desired rate
   * after an upscaling event.
   *
   * <pre>{@code
   * public class SimpleRateLimitPolicyFactory implements GoogleAdsV17.RateLimitPolicyFactory {
   *   private static final GoogleAdsV17.RateLimitPolicy POLICY =
   *       new GoogleAdsV17.SimpleRateLimitPolicy(1.0 / 1000.0);
   *
   *   @Override
   *   public GoogleAdsV17.RateLimitPolicy getRateLimitPolicy() {
   *     return POLICY;
   *   }
   * }
   * }</pre>
   */
  public static class SimpleRateLimitPolicy implements RateLimitPolicy {
    private final RateLimiter rateLimiter;

    public SimpleRateLimitPolicy(double permitsPerSecond) {
      rateLimiter = RateLimiter.create(permitsPerSecond);
    }

    public SimpleRateLimitPolicy(double permitsPerSecond, long warmupPeriod, TimeUnit unit) {
      rateLimiter = RateLimiter.create(permitsPerSecond, warmupPeriod, unit);
    }

    @Override
    public void onBeforeRequest(@Nullable String developerToken, String customerId, Message request)
        throws InterruptedException {
      rateLimiter.acquire();
    }

    @Override
    public void onSuccess(@Nullable String developerToken, String customerId, Message request) {}

    @Override
    public void onError(
        @Nullable String developerToken,
        String customerId,
        Message request,
        GoogleAdsError error) {}
  }
}
