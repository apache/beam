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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.google.ads.googleads.v17.errors.AuthenticationErrorEnum.AuthenticationError;
import com.google.ads.googleads.v17.errors.ErrorCode;
import com.google.ads.googleads.v17.errors.ErrorDetails;
import com.google.ads.googleads.v17.errors.GoogleAdsError;
import com.google.ads.googleads.v17.errors.GoogleAdsException;
import com.google.ads.googleads.v17.errors.GoogleAdsFailure;
import com.google.ads.googleads.v17.errors.InternalErrorEnum.InternalError;
import com.google.ads.googleads.v17.errors.QuotaErrorDetails;
import com.google.ads.googleads.v17.errors.QuotaErrorEnum.QuotaError;
import com.google.ads.googleads.v17.services.GoogleAdsRow;
import com.google.ads.googleads.v17.services.SearchGoogleAdsStreamRequest;
import com.google.ads.googleads.v17.services.SearchGoogleAdsStreamResponse;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.protobuf.Duration;
import io.grpc.Metadata;
import io.grpc.Status.Code;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.io.googleads.GoogleAdsV17.RateLimitPolicyFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Enclosed.class)
public class GoogleAdsV17Test {
  static final RateLimitPolicyFactory TEST_POLICY_FACTORY = () -> new DummyRateLimitPolicy();

  @RunWith(JUnit4.class)
  public static class ConstructionTests {
    private final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testReadAllExpandWithDeveloperTokenFromBuilder() {
      pipeline
          .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
          .apply(
              GoogleAdsIO.v17()
                  .readAll()
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken("abc"));
    }

    @Test
    public void testReadAllExpandWithDeveloperTokenFromOptions() {
      pipeline.getOptions().as(GoogleAdsOptions.class).setGoogleAdsDeveloperToken("abc");
      pipeline
          .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
          .apply(GoogleAdsIO.v17().readAll().withRateLimitPolicy(TEST_POLICY_FACTORY));
    }

    @Test
    public void testReadAllExpandWithDeveloperTokenFromOptionsAndBuilder() {
      pipeline.getOptions().as(GoogleAdsOptions.class).setGoogleAdsDeveloperToken("abc");
      pipeline
          .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
          .apply(
              GoogleAdsIO.v17()
                  .readAll()
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken(null));
    }

    @Test
    public void testReadAllExpandWithoutDeveloperToken() throws Exception {
      Assert.assertThrows(
          "Developer token required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
                  .apply(GoogleAdsIO.v17().readAll().withRateLimitPolicy(TEST_POLICY_FACTORY)));
    }

    @Test
    public void testReadAllExpandWithoutRateLimitPolicy() throws Exception {
      Assert.assertThrows(
          "Rate limit policy required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
                  .apply(GoogleAdsIO.v17().readAll().withDeveloperToken("abc")));
    }

    @Test
    public void testReadAllExpandWithoutValidGoogleAdsClientFactory() throws Exception {
      Assert.assertThrows(
          "Non-null googleAdsClientFactory required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
                  .apply(
                      GoogleAdsIO.v17()
                          .readAll()
                          .withRateLimitPolicy(TEST_POLICY_FACTORY)
                          .withGoogleAdsClientFactory(null)));
    }

    @Test
    public void testReadAllExpandWithoutValidRateLimitPolicy() throws Exception {
      Assert.assertThrows(
          "Non-null rateLimitPolicy required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(new TypeDescriptor<SearchGoogleAdsStreamRequest>() {}))
                  .apply(GoogleAdsIO.v17().readAll().withRateLimitPolicy(null)));
    }

    @Test
    public void testReadExpandWithDeveloperTokenFromBuilder() {
      pipeline
          .apply(Create.empty(TypeDescriptors.strings()))
          .apply(
              GoogleAdsIO.v17()
                  .read()
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken("abc")
                  .withQuery("GAQL"));
      pipeline.getOptions().as(GoogleAdsOptions.class).setGoogleAdsDeveloperToken("abc");
      pipeline
          .apply(Create.empty(TypeDescriptors.strings()))
          .apply(
              GoogleAdsIO.v17().read().withRateLimitPolicy(TEST_POLICY_FACTORY).withQuery("GAQL"));
    }

    @Test
    public void testReadExpandWithDeveloperTokenFromOptions() {
      pipeline.getOptions().as(GoogleAdsOptions.class).setGoogleAdsDeveloperToken("abc");
      pipeline
          .apply(Create.empty(TypeDescriptors.strings()))
          .apply(
              GoogleAdsIO.v17().read().withRateLimitPolicy(TEST_POLICY_FACTORY).withQuery("GAQL"));
    }

    @Test
    public void testReadExpandWithDeveloperTokenFromOptionsAndBuilder() {
      pipeline.getOptions().as(GoogleAdsOptions.class).setGoogleAdsDeveloperToken("abc");
      pipeline
          .apply(Create.empty(TypeDescriptors.strings()))
          .apply(
              GoogleAdsIO.v17()
                  .read()
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken(null)
                  .withQuery("GAQL"));
    }

    @Test
    public void testReadExpandWithoutDeveloperToken() throws Exception {
      Assert.assertThrows(
          "Developer token required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(
                      GoogleAdsIO.v17()
                          .read()
                          .withRateLimitPolicy(TEST_POLICY_FACTORY)
                          .withQuery("GAQL")));
    }

    @Test
    public void testReadExpandWithoutQuery() throws Exception {
      Assert.assertThrows(
          "Query required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(GoogleAdsIO.v17().read().withRateLimitPolicy(TEST_POLICY_FACTORY)));
    }

    @Test
    public void testReadExpandWithoutRateLimitPolicy() throws Exception {
      Assert.assertThrows(
          "Rate limit policy required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(GoogleAdsIO.v17().read().withDeveloperToken("abc").withQuery("GAQL")));
    }

    @Test
    public void testReadExpandWithoutValidGoogleAdsClientFactory() throws Exception {
      Assert.assertThrows(
          "Non-null googleAdsClientFactory required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(
                      GoogleAdsIO.v17()
                          .read()
                          .withRateLimitPolicy(TEST_POLICY_FACTORY)
                          .withQuery("GAQL")
                          .withGoogleAdsClientFactory(null)));
    }

    @Test
    public void testReadExpandWithoutValidQuery() throws Exception {
      Assert.assertThrows(
          "Non-null query required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(
                      GoogleAdsIO.v17()
                          .read()
                          .withRateLimitPolicy(TEST_POLICY_FACTORY)
                          .withQuery(null)));

      Assert.assertThrows(
          "Non-empty query required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(
                      GoogleAdsIO.v17()
                          .read()
                          .withRateLimitPolicy(TEST_POLICY_FACTORY)
                          .withQuery("")));
    }

    @Test
    public void testReadExpandWithoutValidRateLimitPolicy() throws Exception {
      Assert.assertThrows(
          "Non-null rateLimitPolicy required but not provided",
          IllegalArgumentException.class,
          () ->
              pipeline
                  .apply(Create.empty(TypeDescriptors.strings()))
                  .apply(GoogleAdsIO.v17().read().withQuery("GAQL").withRateLimitPolicy(null)));
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class ExecutionTests {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Before
    public void init() {
      GoogleAdsOptions options = pipeline.getOptions().as(GoogleAdsOptions.class);
      options.setGoogleAdsCredentialFactoryClass(NoopCredentialFactory.class);
      synchronized (GoogleAdsV17.ReadAll.ReadAllFn.class) {
        GoogleAdsV17.ReadAll.ReadAllFn.sleeper = (long millis) -> {};
      }
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRead() {
      when(MockGoogleAdsClientFactory.GOOGLE_ADS_SERVICE_STUB_V17
              .searchStreamCallable()
              .call(any(SearchGoogleAdsStreamRequest.class))
              .iterator())
          .thenReturn(
              ImmutableList.<SearchGoogleAdsStreamResponse>of(
                      SearchGoogleAdsStreamResponse.newBuilder()
                          .addResults(GoogleAdsRow.newBuilder())
                          .build())
                  .iterator());

      PCollection<GoogleAdsRow> rows =
          pipeline
              .apply(Create.of("123"))
              .apply(
                  GoogleAdsIO.v17()
                      .read()
                      .withGoogleAdsClientFactory(new MockGoogleAdsClientFactory())
                      .withRateLimitPolicy(TEST_POLICY_FACTORY)
                      .withDeveloperToken("abc")
                      .withQuery("GAQL"));
      PAssert.thatSingleton(rows).isEqualTo(GoogleAdsRow.getDefaultInstance());

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadWithFailureFromMaxRetriesExceeded() throws Exception {
      when(MockGoogleAdsClientFactory.GOOGLE_ADS_SERVICE_STUB_V17
              .searchStreamCallable()
              .call(any(SearchGoogleAdsStreamRequest.class)))
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setInternalError(InternalError.TRANSIENT_ERROR)))
                      .build(),
                  new Metadata()));

      pipeline
          .apply(Create.of("123"))
          .apply(
              GoogleAdsIO.v17()
                  .read()
                  .withGoogleAdsClientFactory(new MockGoogleAdsClientFactory())
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken("abc")
                  .withQuery("GAQL"));

      PipelineExecutionException exception =
          Assert.assertThrows(
              "Last retryable error after max retries",
              Pipeline.PipelineExecutionException.class,
              pipeline::run);
      Assert.assertEquals(IOException.class, exception.getCause().getClass());
      Assert.assertEquals(
          "Unable to get Google Ads response after retrying 5 times using query (GAQL)",
          exception.getCause().getMessage());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadWithFailureFromNonRetryableError() throws Exception {
      when(MockGoogleAdsClientFactory.GOOGLE_ADS_SERVICE_STUB_V17
              .searchStreamCallable()
              .call(any(SearchGoogleAdsStreamRequest.class)))
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setAuthenticationError(
                                          AuthenticationError.OAUTH_TOKEN_REVOKED)))
                      .build(),
                  new Metadata()));

      pipeline
          .apply(Create.of("123"))
          .apply(
              GoogleAdsIO.v17()
                  .read()
                  .withGoogleAdsClientFactory(new MockGoogleAdsClientFactory())
                  .withRateLimitPolicy(TEST_POLICY_FACTORY)
                  .withDeveloperToken("abc")
                  .withQuery("GAQL"));

      PipelineExecutionException exception =
          Assert.assertThrows(
              "First non-retryable error",
              Pipeline.PipelineExecutionException.class,
              pipeline::run);
      Assert.assertEquals(IOException.class, exception.getCause().getClass());
      Assert.assertEquals(
          "com.google.ads.googleads.v17.errors.GoogleAdsException: errors {\n"
              + "  error_code {\n"
              + "    authentication_error: OAUTH_TOKEN_REVOKED\n"
              + "  }\n"
              + "}\n",
          exception.getCause().getMessage());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadWithRecoveryFromInternalError() throws Exception {
      when(MockGoogleAdsClientFactory.GOOGLE_ADS_SERVICE_STUB_V17
              .searchStreamCallable()
              .call(any(SearchGoogleAdsStreamRequest.class))
              .iterator())
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setInternalError(InternalError.INTERNAL_ERROR)))
                      .build(),
                  new Metadata()))
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setInternalError(InternalError.TRANSIENT_ERROR)))
                      .build(),
                  new Metadata()))
          .thenReturn(
              ImmutableList.<SearchGoogleAdsStreamResponse>of(
                      SearchGoogleAdsStreamResponse.newBuilder()
                          .addResults(GoogleAdsRow.newBuilder())
                          .build())
                  .iterator());

      PCollection<GoogleAdsRow> rows =
          pipeline
              .apply(Create.of("123"))
              .apply(
                  GoogleAdsIO.v17()
                      .read()
                      .withGoogleAdsClientFactory(new MockGoogleAdsClientFactory())
                      .withRateLimitPolicy(TEST_POLICY_FACTORY)
                      .withDeveloperToken("abc")
                      .withQuery("GAQL"));
      PAssert.thatSingleton(rows).isEqualTo(GoogleAdsRow.getDefaultInstance());

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadWithRecoveryFromQuotaErrorWithRetryDelay() throws Exception {
      when(MockGoogleAdsClientFactory.GOOGLE_ADS_SERVICE_STUB_V17
              .searchStreamCallable()
              .call(any(SearchGoogleAdsStreamRequest.class))
              .iterator())
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setQuotaError(QuotaError.RESOURCE_EXHAUSTED))
                              .setDetails(
                                  ErrorDetails.newBuilder()
                                      .setQuotaErrorDetails(
                                          QuotaErrorDetails.newBuilder()
                                              .setRetryDelay(Duration.newBuilder().setSeconds(0)))))
                      .build(),
                  new Metadata()))
          .thenThrow(
              new GoogleAdsException(
                  new ApiException(null, GrpcStatusCode.of(Code.UNKNOWN), false),
                  GoogleAdsFailure.newBuilder()
                      .addErrors(
                          GoogleAdsError.newBuilder()
                              .setErrorCode(
                                  ErrorCode.newBuilder()
                                      .setQuotaError(QuotaError.RESOURCE_EXHAUSTED))
                              .setDetails(
                                  ErrorDetails.newBuilder()
                                      .setQuotaErrorDetails(
                                          QuotaErrorDetails.newBuilder()
                                              .setRetryDelay(
                                                  Duration.newBuilder().setSeconds(42)))))
                      .build(),
                  new Metadata()))
          .thenReturn(
              ImmutableList.<SearchGoogleAdsStreamResponse>of(
                      SearchGoogleAdsStreamResponse.newBuilder()
                          .addResults(GoogleAdsRow.newBuilder())
                          .build())
                  .iterator());

      PCollection<GoogleAdsRow> rows =
          pipeline
              .apply(Create.of("123"))
              .apply(
                  GoogleAdsIO.v17()
                      .read()
                      .withGoogleAdsClientFactory(new MockGoogleAdsClientFactory())
                      .withRateLimitPolicy(TEST_POLICY_FACTORY)
                      .withDeveloperToken("abc")
                      .withQuery("GAQL"));
      PAssert.thatSingleton(rows).isEqualTo(GoogleAdsRow.getDefaultInstance());

      pipeline.run();
    }
  }
}
