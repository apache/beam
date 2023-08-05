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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v14.errors.GoogleAdsError;
import com.google.ads.googleads.v14.errors.GoogleAdsException;
import com.google.ads.googleads.v14.errors.GoogleAdsFailure;
import com.google.ads.googleads.v14.errors.InternalErrorEnum;
import com.google.ads.googleads.v14.errors.QuotaErrorEnum;
import com.google.ads.googleads.v14.services.GoogleAdsRow;
import com.google.ads.googleads.v14.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v14.services.SearchGoogleAdsStreamRequest;
import com.google.ads.googleads.v14.services.SearchGoogleAdsStreamResponse;
import com.google.auto.value.AutoValue;
import com.google.protobuf.Message;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.RateLimiter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * {@link GoogleAdsV14} provides an API to read Google Ads API v14 reports.
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
 * <p>Use {@link GoogleAdsV14#read()} to read a bounded {@link PCollection} of {@link GoogleAdsRow}
 * from a query using {@link Read#withQuery(String)} and one or a few customer IDs using either
 * {@link Read#withCustomerId(Long)} or {@link Read#withCustomerIds(List)}. Alternatively, use
 * {@link GoogleAdsV14#readAll()} to read either a bounded or unbounded {@link PCollection} of
 * {@link GoogleAdsRow} from a {@link PCollection} of {@link SearchGoogleAdsStreamRequest}.
 *
 * <p>For example, using {@link GoogleAdsV14#read()}:
 *
 * <pre>{@code
 * Pipeline p = Pipeline.create();
 * PCollection<GoogleAdsRow> rows =
 *     p.apply(
 *         GoogleAdsIO.v14()
 *             .read()
 *             .withCustomerId(1234567890l)
 *             .withQuery(
 *                 "SELECT"
 *                     + "campaign.id,"
 *                     + "campaign.name,"
 *                     + "campaign.status"
 *                     + "FROM campaign"));
 * p.run();
 * }</pre>
 *
 * <p>Alternatively, using {@link GoogleAdsV14#readAll()} to execute requests from a {@link
 * PCollection} of {@link SearchGoogleAdsStreamRequest}:
 *
 * <pre>{@code
 * Pipeline p = Pipeline.create();
 * PCollection<SearchGoogleAdsStreamRequest> requests =
 *     p.apply(
 *         Create.of(
 *             ImmutableList.of(
 *                 SearchGoogleAdsStreamRequest.newBuilder()
 *                     .setCustomerId(Long.toString(1234567890l))
 *                     .setQuery(
 *                         "SELECT"
 *                             + "campaign.id,"
 *                             + "campaign.name,"
 *                             + "campaign.status"
 *                             + "FROM campaign")
 *                     .build())));
 * PCollection<GoogleAdsRow> rows = requests.apply(GoogleAdsIO.v14().readAll());
 * p.run();
 * }</pre>
 *
 * <h2>Client-side rate limiting</h2>
 *
 * On construction of a {@link GoogleAdsV14#read()} or {@link GoogleAdsV14#readAll()} transform a
 * default rate limiting policy is provided to stay well under the rate limit for the Google Ads
 * API, but this limit is only local to a single worker and operates without any knowledge of other
 * applications using the same developer token for any customer ID. The Google Ads API enforces
 * global limits from the developer token down to the customer ID and it is recommended to host a
 * shared rate limiting service to coordinate traffic to the Google Ads API across all applications
 * using the same developer token. Users of these transforms are strongly advised to implement their
 * own {@link RateLimitPolicy} and {@link RateLimitPolicyFactory} to interact with a shared rate
 * limiting service for any production workloads.
 *
 * @see GoogleAdsIO#v14()
 * @see GoogleAdsOptions
 * @see <a href="https://developers.google.com/google-ads/api/docs/best-practices/overview">Best
 *     Practices in the Google Ads documentation</a>
 */
public class GoogleAdsV14 {
  static final GoogleAdsV14 INSTANCE = new GoogleAdsV14();

  private GoogleAdsV14() {}

  public Read read() {
    return new AutoValue_GoogleAdsV14_Read.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .setRateLimitPolicyFactory(() -> new DefaultRateLimitPolicy())
        .build();
  }

  public ReadAll readAll() {
    return new AutoValue_GoogleAdsV14_ReadAll.Builder()
        .setGoogleAdsClientFactory(DefaultGoogleAdsClientFactory.getInstance())
        .setRateLimitPolicyFactory(() -> new DefaultRateLimitPolicy())
        .build();
  }

  /**
   * A {@link PTransform} that reads the results of a Google Ads query as {@link GoogleAdsRow}
   * objects.
   *
   * @see GoogleAdsIO#v14()
   * @see #readAll()
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<GoogleAdsRow>> {
    abstract @Nullable String getDeveloperToken();

    abstract @Nullable Long getLoginCustomerId();

    abstract @Nullable List<Long> getCustomerIds();

    abstract @Nullable String getQuery();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDeveloperToken(@Nullable String developerToken);

      abstract Builder setLoginCustomerId(@Nullable Long loginCustomerId);

      abstract Builder setCustomerIds(List<Long> customerId);

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
     * Creates and returns a new {@link Read} transform with the specified customer IDs to query.
     *
     * @param customerIds
     * @return A new {@link Read} transform with the specified customer IDs to query.
     * @see SearchGoogleAdsStreamRequest
     * @see #withQuery(String)
     */
    public Read withCustomerIds(List<Long> customerIds) {
      checkArgumentNotNull(customerIds, "customerIds cannot be null");
      checkArgument(customerIds.size() > 0, "customerIds cannot be empty");

      return toBuilder().setCustomerIds(ImmutableList.copyOf(customerIds)).build();
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified customer ID to query.
     *
     * @param customerId
     * @return A new {@link Read} transform with the specified customer ID to query.
     * @see SearchGoogleAdsStreamRequest
     * @see #withQuery(String)
     */
    public Read withCustomerId(Long customerId) {
      checkArgumentNotNull(customerId, "customerId cannot be null");

      return withCustomerIds(ImmutableList.of(customerId));
    }

    /**
     * Creates and returns a new {@link Read} transform with the specified query. The query will be
     * executed for each customer ID.
     *
     * @param query
     * @return A new {@link Read} transform with the specified query.
     * @see SearchGoogleAdsStreamRequest
     * @see #withCustomerId(Long)
     * @see #withCustomerIds(List)
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
    public PCollection<GoogleAdsRow> expand(PBegin input) {
      String query = getQuery();
      List<Long> customerIds = getCustomerIds();
      checkArgumentNotNull(query, "withQuery() is required");
      checkArgumentNotNull(customerIds, "either withCustomerId() or withCustomerIds() is required");

      return input
          .apply(Create.of(customerIds))
          .apply(
              MapElements.into(TypeDescriptor.of(SearchGoogleAdsStreamRequest.class))
                  .via(
                      customerId ->
                          SearchGoogleAdsStreamRequest.newBuilder()
                              .setCustomerId(Long.toString(customerId))
                              .setQuery(query)
                              .build()))
          .apply(
              INSTANCE
                  .readAll()
                  .withDeveloperToken(getDeveloperToken())
                  .withLoginCustomerId(getLoginCustomerId())
                  .withGoogleAdsClientFactory(getGoogleAdsClientFactory())
                  .withRateLimitPolicy(getRateLimitPolicyFactory()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotDefault(
              DisplayData.item("customerIds", String.valueOf(getCustomerIds()))
                  .withLabel("Customer IDs"),
              "null")
          .addIfNotNull(DisplayData.item("query", String.valueOf(getQuery())).withLabel("Query"));
    }
  }

  /**
   * A {@link PTransform} that reads the results of many {@link SearchGoogleAdsStreamRequest}
   * objects as {@link GoogleAdsRow} objects. *
   *
   * @see GoogleAdsIO#v14()
   * @see #readAll()
   */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<SearchGoogleAdsStreamRequest>, PCollection<GoogleAdsRow>> {
    abstract @Nullable String getDeveloperToken();

    abstract @Nullable Long getLoginCustomerId();

    abstract GoogleAdsClientFactory getGoogleAdsClientFactory();

    abstract RateLimitPolicyFactory getRateLimitPolicyFactory();

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

      return input.apply(ParDo.of(new ReadAllFn(this)));
    }

    /**
     * A {@link DoFn} that reads reports from Google Ads for each query using the {@code
     * SearchStream} method.
     */
    @VisibleForTesting
    static class ReadAllFn extends DoFn<SearchGoogleAdsStreamRequest, GoogleAdsRow> {
      private static final int MAX_RETRIES = 5;
      private static final FluentBackoff BACKOFF =
          FluentBackoff.DEFAULT
              .withExponent(2.0)
              .withInitialBackoff(Duration.standardSeconds(30))
              .withMaxRetries(MAX_RETRIES);

      @VisibleForTesting static Sleeper sleeper = Sleeper.DEFAULT;

      private final GoogleAdsV14.ReadAll spec;

      private transient @Nullable GoogleAdsClient googleAdsClient;
      private transient @Nullable GoogleAdsServiceClient googleAdsServiceClient;
      private transient @Nullable RateLimitPolicy rateLimitPolicy;

      ReadAllFn(GoogleAdsV14.ReadAll spec) {
        this.spec = spec;
      }

      @Setup
      public void setup(PipelineOptions options) {
        GoogleAdsOptions adsOptions = options.as(GoogleAdsOptions.class);

        googleAdsClient =
            spec.getGoogleAdsClientFactory()
                .newGoogleAdsClient(
                    adsOptions, spec.getDeveloperToken(), null, spec.getLoginCustomerId());
        googleAdsServiceClient = googleAdsClient.getVersion14().createGoogleAdsServiceClient();
        rateLimitPolicy = spec.getRateLimitPolicyFactory().getRateLimitPolicy();
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException, InterruptedException {
        GoogleAdsClient googleAdsClient = checkStateNotNull(this.googleAdsClient);
        GoogleAdsServiceClient googleAdsServiceClient =
            checkStateNotNull(this.googleAdsServiceClient);
        RateLimitPolicy rateLimitPolicy = checkStateNotNull(this.rateLimitPolicy);

        BackOff backoff = BACKOFF.backoff();
        BackOff nextBackoff = backoff;
        GoogleAdsException lastException = null;

        SearchGoogleAdsStreamRequest request = c.element();
        String developerToken = googleAdsClient.getDeveloperToken();
        String customerId = request.getCustomerId();

        do {
          rateLimitPolicy.onBeforeRequest(developerToken, customerId, request);

          try {
            for (SearchGoogleAdsStreamResponse response :
                googleAdsServiceClient.searchStreamCallable().call(request)) {
              for (GoogleAdsRow row : response.getResultsList()) {
                c.output(row);
              }
            }
            rateLimitPolicy.onSuccess(developerToken, customerId, request);
            return;
          } catch (GoogleAdsException e) {
            GoogleAdsError retryableError =
                findFirstRetryableError(e.getGoogleAdsFailure())
                    .orElseThrow(() -> new IOException(e));

            rateLimitPolicy.onError(developerToken, customerId, request, retryableError);

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
    void onBeforeRequest(String developerToken, String customerId, Message request)
        throws InterruptedException;

    /**
     * Called after a request succeeds.
     *
     * @param developerToken The developer token used for the request.
     * @param customerId The customer ID specified on the request.
     * @param request Any Google Ads API request.
     */
    void onSuccess(String developerToken, String customerId, Message request);

    /**
     * Called after a request fails with a retryable error.
     *
     * @param developerToken The developer token used for the request.
     * @param customerId The customer ID specified on the request.
     * @param request Any Google Ads API request.
     * @param error A retryable error.
     */
    void onError(String developerToken, String customerId, Message request, GoogleAdsError error);
  }

  static class DefaultRateLimitPolicy implements RateLimitPolicy {
    private static final RateLimiter RATE_LIMITER = RateLimiter.create(1.0);

    DefaultRateLimitPolicy() {}

    @Override
    public void onBeforeRequest(String developerToken, String customerId, Message request)
        throws InterruptedException {
      RATE_LIMITER.acquire();
    }

    @Override
    public void onSuccess(String developerToken, String customerId, Message request) {}

    @Override
    public void onError(
        String developerToken, String customerId, Message request, GoogleAdsError error) {}
  }
}
