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
package org.apache.beam.sdk.extensions.opentelemetry.gcp.auth;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GcpAuthAutoConfigurationCustomizerProvider.GCP_USER_PROJECT_ID_KEY;
import static org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GcpAuthAutoConfigurationCustomizerProvider.QUOTA_USER_PROJECT_HEADER;
import static org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GcpAuthAutoConfigurationCustomizerProvider.SIGNAL_TYPE_ALL;
import static org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GcpAuthAutoConfigurationCustomizerProvider.SIGNAL_TYPE_METRICS;
import static org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GcpAuthAutoConfigurationCustomizerProvider.SIGNAL_TYPE_TRACES;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.common.ComponentLoader;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.autoconfigure.internal.SpiHelper;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigurationException;
import io.opentelemetry.sdk.autoconfigure.spi.metrics.ConfigurableMetricExporterProvider;
import io.opentelemetry.sdk.autoconfigure.spi.traces.ConfigurableSpanExporterProvider;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

/**
 * Copied from open-telemetry. Link:
 * https://github.com/open-telemetry/opentelemetry-java-contrib/blob/main/gcp-auth-extension/src/test/java/io/opentelemetry/contrib/gcp/auth/GcpAuthAutoConfigurationCustomizerProviderTest.java
 */
class GcpAuthAutoConfigurationCustomizerProviderTest {

  private static final String DUMMY_GCP_RESOURCE_PROJECT_ID = "my-gcp-resource-project-id";
  private static final String DUMMY_GCP_QUOTA_PROJECT_ID = "my-gcp-quota-project-id";
  private static final Random TEST_RANDOM = new Random();

  @Mock private GoogleCredentials mockedGoogleCredentials;

  @Captor private ArgumentCaptor<Supplier<Map<String, String>>> traceHeaderSupplierCaptor;
  @Captor private ArgumentCaptor<Supplier<Map<String, String>>> metricHeaderSupplierCaptor;

  private static final ImmutableMap<String, String> DEFAULT_OTEL_PROPERTIES_SPAN_EXPORTER =
      ImmutableMap.<String, String>builder()
          .put("otel.exporter.otlp.traces.endpoint", "https://telemetry.googleapis.com/v1/traces")
          .put("otel.traces.exporter", "otlp")
          .put("otel.metrics.exporter", "none")
          .put("otel.logs.exporter", "none")
          .put("otel.resource.attributes", "foo=bar")
          .build();

  private static final ImmutableMap<String, String> DEFAULT_OTEL_PROPERTIES_METRIC_EXPORTER =
      ImmutableMap.<String, String>builder()
          .put("otel.exporter.otlp.metrics.endpoint", "https://telemetry.googleapis.com/v1/metrics")
          .put("otel.traces.exporter", "none")
          .put("otel.metrics.exporter", "otlp")
          .put("otel.logs.exporter", "none")
          .put("otel.resource.attributes", "foo=bar")
          .build();

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  public void teardown() {
    System.clearProperty(ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty());
    System.clearProperty(ConfigurableOption.GOOGLE_CLOUD_QUOTA_PROJECT.getSystemProperty());
    System.clearProperty(ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty());
  }

  // TODO: Use parameterized test for testing traces customizer for http & grpc.
  @Test
  void testTraceCustomizerOtlpHttp() {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(), SIGNAL_TYPE_TRACES);
    // Prepare mocks
    prepareMockBehaviorForGoogleCredentials();
    OtlpHttpSpanExporter mockOtlpHttpSpanExporter = mock(OtlpHttpSpanExporter.class);
    OtlpHttpSpanExporterBuilder otlpSpanExporterBuilder = OtlpHttpSpanExporter.builder();
    OtlpHttpSpanExporterBuilder spyOtlpHttpSpanExporterBuilder =
        Mockito.spy(otlpSpanExporterBuilder);
    when(spyOtlpHttpSpanExporterBuilder.build()).thenReturn(mockOtlpHttpSpanExporter);

    when(mockOtlpHttpSpanExporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());
    List<SpanData> exportedSpans = new ArrayList<>();
    when(mockOtlpHttpSpanExporter.export(any()))
        .thenAnswer(
            invocationOnMock -> {
              exportedSpans.addAll(invocationOnMock.getArgument(0));
              return CompletableResultCode.ofSuccess();
            });
    Mockito.when(mockOtlpHttpSpanExporter.toBuilder()).thenReturn(spyOtlpHttpSpanExporterBuilder);

    // begin assertions
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpHttpSpanExporter);
      generateTestSpan(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();

      Mockito.verify(mockOtlpHttpSpanExporter, Mockito.times(1)).toBuilder();
      Mockito.verify(spyOtlpHttpSpanExporterBuilder, Mockito.times(1))
          .setHeaders(traceHeaderSupplierCaptor.capture());
      assertThat(traceHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
      assertThat(authHeadersQuotaProjectIsPresent(traceHeaderSupplierCaptor.getValue().get()))
          .isTrue();

      Mockito.verify(mockOtlpHttpSpanExporter, Mockito.atLeast(1)).export(Mockito.anyCollection());

      assertThat(exportedSpans).isNotEmpty();
      for (SpanData spanData : exportedSpans) {
        assertThat(spanData.getResource().getAttributes().asMap())
            .containsEntry(
                AttributeKey.stringKey(GCP_USER_PROJECT_ID_KEY), DUMMY_GCP_RESOURCE_PROJECT_ID);
        assertThat(spanData.getResource().getAttributes().asMap())
            .containsEntry(AttributeKey.stringKey("foo"), "bar");
        assertThat(spanData.getAttributes().asMap()).containsKey(AttributeKey.longKey("work_loop"));
      }
    }
  }

  @Test
  void testTraceCustomizerOtlpGrpc() {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(), SIGNAL_TYPE_TRACES);
    // Prepare mocks
    prepareMockBehaviorForGoogleCredentials();
    OtlpGrpcSpanExporter mockOtlpGrpcSpanExporter = Mockito.mock(OtlpGrpcSpanExporter.class);
    OtlpGrpcSpanExporterBuilder spyOtlpGrpcSpanExporterBuilder =
        Mockito.spy(OtlpGrpcSpanExporter.builder());
    List<SpanData> exportedSpans = new ArrayList<>();
    configureGrpcMockSpanExporter(
        mockOtlpGrpcSpanExporter, spyOtlpGrpcSpanExporterBuilder, exportedSpans);

    // begin assertions
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpGrpcSpanExporter);
      generateTestSpan(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();

      Mockito.verify(mockOtlpGrpcSpanExporter, Mockito.times(1)).toBuilder();
      Mockito.verify(spyOtlpGrpcSpanExporterBuilder, Mockito.times(1))
          .setHeaders(traceHeaderSupplierCaptor.capture());
      assertThat(traceHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
      assertThat(authHeadersQuotaProjectIsPresent(traceHeaderSupplierCaptor.getValue().get()))
          .isTrue();

      Mockito.verify(mockOtlpGrpcSpanExporter, Mockito.atLeast(1)).export(Mockito.anyCollection());

      assertThat(exportedSpans).isNotEmpty();
      for (SpanData spanData : exportedSpans) {
        assertThat(spanData.getResource().getAttributes().asMap())
            .containsEntry(
                AttributeKey.stringKey(GCP_USER_PROJECT_ID_KEY), DUMMY_GCP_RESOURCE_PROJECT_ID);
        assertThat(spanData.getResource().getAttributes().asMap())
            .containsEntry(AttributeKey.stringKey("foo"), "bar");
        assertThat(spanData.getAttributes().asMap()).containsKey(AttributeKey.longKey("work_loop"));
      }
    }
  }

  @Test
  void testMetricCustomizerOtlpHttp() {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(),
        SIGNAL_TYPE_METRICS);
    // Prepare mocks
    prepareMockBehaviorForGoogleCredentials();
    OtlpHttpMetricExporter mockOtlpHttpMetricExporter = Mockito.mock(OtlpHttpMetricExporter.class);
    OtlpHttpMetricExporterBuilder otlpMetricExporterBuilder = OtlpHttpMetricExporter.builder();
    OtlpHttpMetricExporterBuilder spyOtlpHttpMetricExporterBuilder =
        Mockito.spy(otlpMetricExporterBuilder);
    List<MetricData> exportedMetrics = new ArrayList<>();
    configureHttpMockMetricExporter(
        mockOtlpHttpMetricExporter, spyOtlpHttpMetricExporterBuilder, exportedMetrics);

    // begin assertions
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpHttpMetricExporter);
      generateTestMetric(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();

      Mockito.verify(mockOtlpHttpMetricExporter, Mockito.times(1)).toBuilder();
      Mockito.verify(spyOtlpHttpMetricExporterBuilder, Mockito.times(1))
          .setHeaders(metricHeaderSupplierCaptor.capture());
      assertThat(metricHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
      assertThat(authHeadersQuotaProjectIsPresent(metricHeaderSupplierCaptor.getValue().get()))
          .isTrue();

      Mockito.verify(mockOtlpHttpMetricExporter, Mockito.atLeast(1))
          .export(Mockito.anyCollection());

      assertThat(exportedMetrics).isNotEmpty();
      for (MetricData metricData : exportedMetrics) {
        assertThat(metricData.getResource().getAttributes().asMap())
            .containsEntry(
                AttributeKey.stringKey(GCP_USER_PROJECT_ID_KEY), DUMMY_GCP_RESOURCE_PROJECT_ID);
        assertThat(metricData.getResource().getAttributes().asMap())
            .containsEntry(AttributeKey.stringKey("foo"), "bar");
        assertThat(metricData.getLongSumData().getPoints()).isNotEmpty();
        for (io.opentelemetry.sdk.metrics.data.PointData pointData :
            metricData.getLongSumData().getPoints()) {
          assertThat(pointData.getAttributes().asMap())
              .containsKey(AttributeKey.longKey("work_loop"));
        }
      }
    }
  }

  @Test
  void testMetricCustomizerOtlpGrpc() {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(),
        SIGNAL_TYPE_METRICS);
    // Prepare mocks
    prepareMockBehaviorForGoogleCredentials();
    OtlpGrpcMetricExporter mockOtlpGrpcMetricExporter = Mockito.mock(OtlpGrpcMetricExporter.class);
    OtlpGrpcMetricExporterBuilder otlpMetricExporterBuilder = OtlpGrpcMetricExporter.builder();
    OtlpGrpcMetricExporterBuilder spyOtlpGrpcMetricExporterBuilder =
        Mockito.spy(otlpMetricExporterBuilder);
    List<MetricData> exportedMetrics = new ArrayList<>();
    configureGrpcMockMetricExporter(
        mockOtlpGrpcMetricExporter, spyOtlpGrpcMetricExporterBuilder, exportedMetrics);

    // begin assertions
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpGrpcMetricExporter);
      generateTestMetric(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();

      Mockito.verify(mockOtlpGrpcMetricExporter, Mockito.times(1)).toBuilder();
      Mockito.verify(spyOtlpGrpcMetricExporterBuilder, Mockito.times(1))
          .setHeaders(metricHeaderSupplierCaptor.capture());
      assertThat(metricHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
      assertThat(authHeadersQuotaProjectIsPresent(metricHeaderSupplierCaptor.getValue().get()))
          .isTrue();

      Mockito.verify(mockOtlpGrpcMetricExporter, Mockito.atLeast(1))
          .export(Mockito.anyCollection());

      assertThat(exportedMetrics).isNotEmpty();
      for (MetricData metricData : exportedMetrics) {
        assertThat(metricData.getResource().getAttributes().asMap())
            .containsEntry(
                AttributeKey.stringKey(GCP_USER_PROJECT_ID_KEY), DUMMY_GCP_RESOURCE_PROJECT_ID);
        assertThat(metricData.getResource().getAttributes().asMap())
            .containsEntry(AttributeKey.stringKey("foo"), "bar");
        assertThat(metricData.getLongSumData().getPoints()).isNotEmpty();
        for (io.opentelemetry.sdk.metrics.data.PointData pointData :
            metricData.getLongSumData().getPoints()) {
          assertThat(pointData.getAttributes().asMap())
              .containsKey(AttributeKey.longKey("work_loop"));
        }
      }
    }
  }

  @Test
  void testCustomizerFailWithMissingResourceProject() {
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(), SIGNAL_TYPE_ALL);
    OtlpGrpcSpanExporter mockOtlpGrpcSpanExporter = Mockito.mock(OtlpGrpcSpanExporter.class);
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      assertThrows(
          ConfigurationException.class,
          () -> buildOpenTelemetrySdkWithExporter(mockOtlpGrpcSpanExporter));
    }
  }

  @ParameterizedTest
  @MethodSource("provideQuotaBehaviorTestCases")
  @SuppressWarnings("CannotMockMethod")
  void testQuotaProjectBehavior(QuotaProjectIdTestBehavior testCase) throws IOException {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(), SIGNAL_TYPE_ALL);

    // Prepare request metadata
    AccessToken fakeAccessToken = new AccessToken("fake", Date.from(Instant.now()));
    ImmutableMap<String, List<String>> mockedRequestMetadata;
    if (testCase.getIsQuotaProjectPresentInMetadata()) {
      mockedRequestMetadata =
          ImmutableMap.of(
              "Authorization",
              Collections.singletonList("Bearer " + fakeAccessToken.getTokenValue()),
              QUOTA_USER_PROJECT_HEADER,
              Collections.singletonList(DUMMY_GCP_QUOTA_PROJECT_ID));
    } else {
      mockedRequestMetadata =
          ImmutableMap.of(
              "Authorization",
              Collections.singletonList("Bearer " + fakeAccessToken.getTokenValue()));
    }
    // mock credentials to return the prepared request metadata
    Mockito.when(mockedGoogleCredentials.getRequestMetadata()).thenReturn(mockedRequestMetadata);

    // configure environment according to test case
    String quotaProjectId = testCase.getUserSpecifiedQuotaProjectId(); // maybe empty string
    if (quotaProjectId != null) {
      // user specified a quota project id
      System.setProperty(
          ConfigurableOption.GOOGLE_CLOUD_QUOTA_PROJECT.getSystemProperty(), quotaProjectId);
    }

    // prepare mock exporter
    OtlpGrpcSpanExporter mockOtlpGrpcSpanExporter = Mockito.mock(OtlpGrpcSpanExporter.class);
    OtlpGrpcSpanExporterBuilder spyOtlpGrpcSpanExporterBuilder =
        Mockito.spy(OtlpGrpcSpanExporter.builder());
    List<SpanData> exportedSpans = new ArrayList<>();
    configureGrpcMockSpanExporter(
        mockOtlpGrpcSpanExporter, spyOtlpGrpcSpanExporterBuilder, exportedSpans);

    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      // Export telemetry to capture headers in the export calls
      OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpGrpcSpanExporter);
      generateTestSpan(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();
      Mockito.verify(spyOtlpGrpcSpanExporterBuilder, Mockito.times(1))
          .setHeaders(traceHeaderSupplierCaptor.capture());

      // assert that the Authorization bearer token header is present
      Map<String, String> exportHeaders = traceHeaderSupplierCaptor.getValue().get();
      assertThat(exportHeaders).containsEntry("Authorization", "Bearer fake");

      if (testCase.getExpectedQuotaProjectInHeader() == null) {
        // there should be no user quota project header
        assertThat(exportHeaders).doesNotContainKey(QUOTA_USER_PROJECT_HEADER);
      } else {
        // there should be user quota project header with expected value
        assertThat(exportHeaders)
            .containsEntry(QUOTA_USER_PROJECT_HEADER, testCase.getExpectedQuotaProjectInHeader());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("provideProjectIdBehaviorTestCases")
  @SuppressWarnings("CannotMockMethod")
  void testProjectIdBehavior(ProjectIdTestBehavior testCase) throws IOException {
    System.clearProperty(ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty());
    System.clearProperty(ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty());

    // configure environment according to test case
    String userSpecifiedProjectId = testCase.getUserSpecifiedProjectId();
    if (userSpecifiedProjectId != null) {
      System.setProperty(
          ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), userSpecifiedProjectId);
    }
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(), SIGNAL_TYPE_TRACES);

    // prepare request metadata (may or may not be called depending on test scenario)
    AccessToken fakeAccessToken = new AccessToken("fake", Date.from(Instant.now()));
    ImmutableMap<String, List<String>> mockedRequestMetadata =
        ImmutableMap.of(
            "Authorization",
            Collections.singletonList("Bearer " + fakeAccessToken.getTokenValue()));
    Mockito.lenient()
        .when(mockedGoogleCredentials.getRequestMetadata())
        .thenReturn(mockedRequestMetadata);

    // only mock getProjectId() if it will be called (i.e., user didn't specify project ID)
    boolean shouldFallbackToCredentials =
        userSpecifiedProjectId == null || userSpecifiedProjectId.isEmpty();
    if (shouldFallbackToCredentials) {
      Mockito.when(mockedGoogleCredentials.getProjectId())
          .thenReturn(testCase.getCredentialsProjectId());
    }

    // prepare mock exporter
    OtlpGrpcSpanExporter mockOtlpGrpcSpanExporter = Mockito.mock(OtlpGrpcSpanExporter.class);
    OtlpGrpcSpanExporterBuilder spyOtlpGrpcSpanExporterBuilder =
        Mockito.spy(OtlpGrpcSpanExporter.builder());
    List<SpanData> exportedSpans = new ArrayList<>();
    configureGrpcMockSpanExporter(
        mockOtlpGrpcSpanExporter, spyOtlpGrpcSpanExporterBuilder, exportedSpans);

    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      if (testCase.getExpectedToThrow()) {
        // expect exception to be thrown when project ID is not available
        assertThrows(
            ConfigurationException.class,
            () -> buildOpenTelemetrySdkWithExporter(mockOtlpGrpcSpanExporter));
        // verify getProjectId() was called to attempt fallback
        Mockito.verify(mockedGoogleCredentials, Mockito.times(1)).getProjectId();
      } else {
        // export telemetry and verify resource attributes contain expected project ID
        OpenTelemetrySdk sdk = buildOpenTelemetrySdkWithExporter(mockOtlpGrpcSpanExporter);
        generateTestSpan(sdk);
        CompletableResultCode code = sdk.shutdown();
        CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
        assertThat(joinResult.isSuccess()).isTrue();

        assertThat(exportedSpans).isNotEmpty();
        for (SpanData spanData : exportedSpans) {
          assertThat(spanData.getResource().getAttributes().asMap())
              .containsEntry(
                  AttributeKey.stringKey(GCP_USER_PROJECT_ID_KEY),
                  testCase.getExpectedProjectIdInResource());
        }

        // verify whether getProjectId() was called based on whether fallback was needed
        if (shouldFallbackToCredentials) {
          Mockito.verify(mockedGoogleCredentials, Mockito.times(1)).getProjectId();
        } else {
          Mockito.verify(mockedGoogleCredentials, Mockito.never()).getProjectId();
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("provideTargetSignalBehaviorTestCases")
  void testTargetSignalsBehavior(TargetSignalBehavior testCase) {
    // Set resource project system property
    System.setProperty(
        ConfigurableOption.GOOGLE_CLOUD_PROJECT.getSystemProperty(), DUMMY_GCP_RESOURCE_PROJECT_ID);
    // Prepare mocks
    // Prepare mocked credential
    prepareMockBehaviorForGoogleCredentials();

    // Prepare mocked span exporter
    OtlpGrpcSpanExporter mockOtlpGrpcSpanExporter = Mockito.mock(OtlpGrpcSpanExporter.class);
    OtlpGrpcSpanExporterBuilder spyOtlpGrpcSpanExporterBuilder =
        Mockito.spy(OtlpGrpcSpanExporter.builder());
    List<SpanData> exportedSpans = new ArrayList<>();
    configureGrpcMockSpanExporter(
        mockOtlpGrpcSpanExporter, spyOtlpGrpcSpanExporterBuilder, exportedSpans);
    configureGrpcMockSpanExporter(
        mockOtlpGrpcSpanExporter, spyOtlpGrpcSpanExporterBuilder, exportedSpans);

    // Prepare mocked metrics exporter
    OtlpGrpcMetricExporter mockOtlpGrpcMetricExporter = Mockito.mock(OtlpGrpcMetricExporter.class);
    OtlpGrpcMetricExporterBuilder otlpMetricExporterBuilder = OtlpGrpcMetricExporter.builder();
    OtlpGrpcMetricExporterBuilder spyOtlpGrpcMetricExporterBuilder =
        Mockito.spy(otlpMetricExporterBuilder);
    List<MetricData> exportedMetrics = new ArrayList<>();
    configureGrpcMockMetricExporter(
        mockOtlpGrpcMetricExporter, spyOtlpGrpcMetricExporterBuilder, exportedMetrics);

    // configure environment according to test case
    System.setProperty(
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty(),
        testCase.getConfiguredTargetSignals());

    // Build Autoconfigured OpenTelemetry SDK using the mocks and send signals
    try (MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
        Mockito.mockStatic(GoogleCredentials.class)) {
      googleCredentialsMockedStatic
          .when(GoogleCredentials::getApplicationDefault)
          .thenReturn(mockedGoogleCredentials);

      OpenTelemetrySdk sdk =
          buildOpenTelemetrySdkWithExporter(
              mockOtlpGrpcSpanExporter,
              mockOtlpGrpcMetricExporter,
              testCase.getUserSpecifiedOtelProperties());
      generateTestMetric(sdk);
      generateTestSpan(sdk);
      CompletableResultCode code = sdk.shutdown();
      CompletableResultCode joinResult = code.join(10, TimeUnit.SECONDS);
      assertThat(joinResult.isSuccess()).isTrue();

      // Check Traces modification conditions
      if (testCase.getExpectedIsTraceSignalModified()) {
        // If traces signal is expected to be modified, auth headers must be present
        Mockito.verify(spyOtlpGrpcSpanExporterBuilder, Mockito.times(1))
            .setHeaders(traceHeaderSupplierCaptor.capture());
        assertThat(traceHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
        assertThat(authHeadersQuotaProjectIsPresent(traceHeaderSupplierCaptor.getValue().get()))
            .isTrue();
      } else {
        // If traces signals is not expected to be modified then no interaction with the builder
        // should be made
        Mockito.verifyNoInteractions(spyOtlpGrpcSpanExporterBuilder);
      }

      // Check Metric modification conditions
      if (testCase.getExpectedIsMetricsSignalModified()) {
        // If metrics signal is expected to be modified, auth headers must be present
        Mockito.verify(spyOtlpGrpcMetricExporterBuilder, Mockito.times(1))
            .setHeaders(metricHeaderSupplierCaptor.capture());
        assertThat(metricHeaderSupplierCaptor.getValue().get().size()).isEqualTo(2);
        assertThat(authHeadersQuotaProjectIsPresent(metricHeaderSupplierCaptor.getValue().get()))
            .isTrue();
      } else {
        // If metrics signals is not expected to be modified then no interaction with the builder
        // should be made
        Mockito.verifyNoInteractions(spyOtlpGrpcMetricExporterBuilder);
      }
    }
  }

  /** Test cases specifying expected behavior for GOOGLE_OTEL_AUTH_TARGET_SIGNALS. */
  private static Stream<Arguments> provideTargetSignalBehaviorTestCases() {
    return Stream.of(
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("traces")
                .setUserSpecifiedOtelProperties(DEFAULT_OTEL_PROPERTIES_SPAN_EXPORTER)
                .setExpectedIsMetricsSignalModified(false)
                .setExpectedIsTraceSignalModified(true)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("metrics")
                .setUserSpecifiedOtelProperties(DEFAULT_OTEL_PROPERTIES_METRIC_EXPORTER)
                .setExpectedIsMetricsSignalModified(true)
                .setExpectedIsTraceSignalModified(false)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("all")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.<String, String>builder()
                        .put(
                            "otel.exporter.otlp.metrics.endpoint",
                            "https://telemetry.googleapis.com/v1/metrics")
                        .put(
                            "otel.exporter.otlp.traces.endpoint",
                            "https://telemetry.googleapis.com/v1/traces")
                        .put("otel.traces.exporter", "otlp")
                        .put("otel.metrics.exporter", "otlp")
                        .put("otel.logs.exporter", "none")
                        .build())
                .setExpectedIsMetricsSignalModified(true)
                .setExpectedIsTraceSignalModified(true)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("metrics, traces")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.<String, String>builder()
                        .put(
                            "otel.exporter.otlp.metrics.endpoint",
                            "https://telemetry.googleapis.com/v1/metrics")
                        .put(
                            "otel.exporter.otlp.traces.endpoint",
                            "https://telemetry.googleapis.com/v1/traces")
                        .put("otel.traces.exporter", "otlp")
                        .put("otel.metrics.exporter", "otlp")
                        .put("otel.logs.exporter", "none")
                        .build())
                .setExpectedIsMetricsSignalModified(true)
                .setExpectedIsTraceSignalModified(true)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.<String, String>builder()
                        .put(
                            "otel.exporter.otlp.metrics.endpoint",
                            "https://telemetry.googleapis.com/v1/metrics")
                        .put(
                            "otel.exporter.otlp.traces.endpoint",
                            "https://telemetry.googleapis.com/v1/traces")
                        .put("otel.traces.exporter", "otlp")
                        .put("otel.metrics.exporter", "otlp")
                        .put("otel.logs.exporter", "none")
                        .build())
                .setExpectedIsMetricsSignalModified(true)
                .setExpectedIsTraceSignalModified(true)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("all")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.of(
                        "otel.exporter.otlp.metrics.endpoint",
                        "https://telemetry.googleapis.com/v1/metrics",
                        "otel.exporter.otlp.traces.endpoint",
                        "https://telemetry.googleapis.com/v1/traces",
                        "otel.traces.exporter",
                        "none",
                        "otel.metrics.exporter",
                        "none",
                        "otel.logs.exporter",
                        "none"))
                .setExpectedIsMetricsSignalModified(false)
                .setExpectedIsTraceSignalModified(false)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("metric, trace")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.<String, String>builder()
                        .put(
                            "otel.exporter.otlp.metrics.endpoint",
                            "https://telemetry.googleapis.com/v1/metrics")
                        .put(
                            "otel.exporter.otlp.traces.endpoint",
                            "https://telemetry.googleapis.com/v1/traces")
                        .put("otel.traces.exporter", "otlp")
                        .put("otel.metrics.exporter", "otlp")
                        .put("otel.logs.exporter", "none")
                        .build())
                .setExpectedIsMetricsSignalModified(false)
                .setExpectedIsTraceSignalModified(false)
                .build()),
        Arguments.of(
            TargetSignalBehavior.builder()
                .setConfiguredTargetSignals("metrics, trace")
                .setUserSpecifiedOtelProperties(
                    ImmutableMap.<String, String>builder()
                        .put(
                            "otel.exporter.otlp.metrics.endpoint",
                            "https://telemetry.googleapis.com/v1/metrics")
                        .put(
                            "otel.exporter.otlp.traces.endpoint",
                            "https://telemetry.googleapis.com/v1/traces")
                        .put("otel.traces.exporter", "otlp")
                        .put("otel.metrics.exporter", "otlp")
                        .put("otel.logs.exporter", "none")
                        .build())
                .setExpectedIsMetricsSignalModified(true)
                .setExpectedIsTraceSignalModified(false)
                .build()));
  }

  /**
   * Test cases specifying expected value for the project ID in the resource given the user input
   * and the current credentials state.
   *
   * <p>{@code null} for {@link ProjectIdTestBehavior#getUserSpecifiedProjectId()} indicates the
   * case of user not specifying the project ID.
   *
   * <p>{@code null} value for {@link ProjectIdTestBehavior#getCredentialsProjectId()} indicates
   * that the mocked credentials are not providing a project ID.
   *
   * <p>{@code true} for {@link ProjectIdTestBehavior#getExpectedToThrow()} indicates the
   * expectation that an exception should be thrown.
   */
  private static Stream<Arguments> provideProjectIdBehaviorTestCases() {
    return Stream.of(
        // User specified project ID takes precedence
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId(DUMMY_GCP_RESOURCE_PROJECT_ID)
                .setCredentialsProjectId("credentials-project-id")
                .setExpectedProjectIdInResource(DUMMY_GCP_RESOURCE_PROJECT_ID)
                .setExpectedToThrow(false)
                .build()),
        // If user specified project ID is empty, fallback to credentials.getProjectId()
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId("")
                .setCredentialsProjectId("credentials-project-id")
                .setExpectedProjectIdInResource("credentials-project-id")
                .setExpectedToThrow(false)
                .build()),
        // If user doesn't specify project ID, fallback to credentials.getProjectId()
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId(null)
                .setCredentialsProjectId("credentials-project-id")
                .setExpectedProjectIdInResource("credentials-project-id")
                .setExpectedToThrow(false)
                .build()),
        // If user doesn't specify and credentials.getProjectId() returns null, throw exception
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId(null)
                .setCredentialsProjectId(null)
                .setExpectedProjectIdInResource(null)
                .setExpectedToThrow(true)
                .build()),
        // If user specified project ID is empty and credentials.getProjectId() returns null, throw
        // exception
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId("")
                .setCredentialsProjectId(null)
                .setExpectedProjectIdInResource(null)
                .setExpectedToThrow(true)
                .build()),
        // If user specifies empty and credentials returns empty (edge case), throw exception
        Arguments.of(
            ProjectIdTestBehavior.builder()
                .setUserSpecifiedProjectId("")
                .setCredentialsProjectId("")
                .setExpectedProjectIdInResource(null)
                .setExpectedToThrow(true)
                .build()));
  }

  /**
   * Test cases specifying expected value for the user quota project header given the user input and
   * the current credentials state.
   *
   * <p>{@code null} for {@link QuotaProjectIdTestBehavior#getUserSpecifiedQuotaProjectId()}
   * indicates the case of user not specifying the quota project ID.
   *
   * <p>{@code null} value for {@link QuotaProjectIdTestBehavior#getExpectedQuotaProjectInHeader()}
   * indicates the expectation that the QUOTA_USER_PROJECT_HEADER should not be present in the
   * export headers.
   *
   * <p>{@code true} for {@link QuotaProjectIdTestBehavior#getIsQuotaProjectPresentInMetadata()}
   * indicates that the mocked credentials are configured to provide DUMMY_GCP_QUOTA_PROJECT_ID as
   * the quota project ID.
   */
  private static Stream<Arguments> provideQuotaBehaviorTestCases() {
    return Stream.of(
        // If quota project present in metadata, it will be used
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId(DUMMY_GCP_QUOTA_PROJECT_ID)
                .setIsQuotaProjectPresentInMetadata(true)
                .setExpectedQuotaProjectInHeader(DUMMY_GCP_QUOTA_PROJECT_ID)
                .build()),
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId("my-custom-quota-project-id")
                .setIsQuotaProjectPresentInMetadata(true)
                .setExpectedQuotaProjectInHeader(DUMMY_GCP_QUOTA_PROJECT_ID)
                .build()),
        // If quota project not present in request metadata, then user specified project is used
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId(DUMMY_GCP_QUOTA_PROJECT_ID)
                .setIsQuotaProjectPresentInMetadata(false)
                .setExpectedQuotaProjectInHeader(DUMMY_GCP_QUOTA_PROJECT_ID)
                .build()),
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId("my-custom-quota-project-id")
                .setIsQuotaProjectPresentInMetadata(false)
                .setExpectedQuotaProjectInHeader("my-custom-quota-project-id")
                .build()),
        // Testing for special edge case inputs
        // user-specified quota project is empty
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId("") // user explicitly specifies empty
                .setIsQuotaProjectPresentInMetadata(true)
                .setExpectedQuotaProjectInHeader(DUMMY_GCP_QUOTA_PROJECT_ID)
                .build()),
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId("")
                .setIsQuotaProjectPresentInMetadata(false)
                .setExpectedQuotaProjectInHeader(null)
                .build()),
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId(null) // user omits specifying quota project
                .setIsQuotaProjectPresentInMetadata(true)
                .setExpectedQuotaProjectInHeader(DUMMY_GCP_QUOTA_PROJECT_ID)
                .build()),
        Arguments.of(
            QuotaProjectIdTestBehavior.builder()
                .setUserSpecifiedQuotaProjectId(null)
                .setIsQuotaProjectPresentInMetadata(false)
                .setExpectedQuotaProjectInHeader(null)
                .build()));
  }

  // Configure necessary behavior on the gRPC mock span exporters to work.
  // Mockito.lenient is used here because this method is used with parameterized tests where based
  // on certain inputs, certain stubbings may not be required.
  private static void configureGrpcMockSpanExporter(
      OtlpGrpcSpanExporter mockGrpcExporter,
      OtlpGrpcSpanExporterBuilder spyGrpcExporterBuilder,
      List<SpanData> exportedSpanContainer) {
    Mockito.lenient().when(spyGrpcExporterBuilder.build()).thenReturn(mockGrpcExporter);
    Mockito.lenient()
        .when(mockGrpcExporter.shutdown())
        .thenReturn(CompletableResultCode.ofSuccess());
    Mockito.lenient().when(mockGrpcExporter.toBuilder()).thenReturn(spyGrpcExporterBuilder);
    Mockito.lenient()
        .when(mockGrpcExporter.export(Mockito.anyCollection()))
        .thenAnswer(
            invocationOnMock -> {
              exportedSpanContainer.addAll(invocationOnMock.getArgument(0));
              return CompletableResultCode.ofSuccess();
            });
  }

  // Configure necessary behavior on the http mock metric exporters to work.
  private static void configureHttpMockMetricExporter(
      OtlpHttpMetricExporter mockOtlpHttpMetricExporter,
      OtlpHttpMetricExporterBuilder spyOtlpHttpMetricExporterBuilder,
      List<MetricData> exportedMetricContainer) {
    Mockito.when(spyOtlpHttpMetricExporterBuilder.build()).thenReturn(mockOtlpHttpMetricExporter);
    Mockito.when(mockOtlpHttpMetricExporter.shutdown())
        .thenReturn(CompletableResultCode.ofSuccess());
    Mockito.when(mockOtlpHttpMetricExporter.toBuilder())
        .thenReturn(spyOtlpHttpMetricExporterBuilder);
    Mockito.when(mockOtlpHttpMetricExporter.export(Mockito.anyCollection()))
        .thenAnswer(
            invocationOnMock -> {
              exportedMetricContainer.addAll(invocationOnMock.getArgument(0));
              return CompletableResultCode.ofSuccess();
            });
    // mock the get default aggregation and aggregation temporality - they're required for valid
    // metric collection.
    Mockito.when(mockOtlpHttpMetricExporter.getDefaultAggregation(Mockito.any()))
        .thenAnswer(
            (Answer<Aggregation>)
                invocationOnMock -> {
                  InstrumentType instrumentType = invocationOnMock.getArgument(0);
                  return OtlpHttpMetricExporter.getDefault().getDefaultAggregation(instrumentType);
                });
    Mockito.when(mockOtlpHttpMetricExporter.getAggregationTemporality(Mockito.any()))
        .thenAnswer(
            (Answer<AggregationTemporality>)
                invocationOnMock -> {
                  InstrumentType instrumentType = invocationOnMock.getArgument(0);
                  return OtlpHttpMetricExporter.getDefault()
                      .getAggregationTemporality(instrumentType);
                });
  }

  // Configure necessary behavior on the gRPC mock metrics exporters to work.
  // Mockito.lenient is used here because this method is used with parameterized tests where based
  // on certain inputs, certain stubbings may not be required.
  private static void configureGrpcMockMetricExporter(
      OtlpGrpcMetricExporter mockOtlpGrpcMetricExporter,
      OtlpGrpcMetricExporterBuilder spyOtlpGrpcMetricExporterBuilder,
      List<MetricData> exportedMetricContainer) {
    Mockito.lenient()
        .when(spyOtlpGrpcMetricExporterBuilder.build())
        .thenReturn(mockOtlpGrpcMetricExporter);
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.shutdown())
        .thenReturn(CompletableResultCode.ofSuccess());
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.toBuilder())
        .thenReturn(spyOtlpGrpcMetricExporterBuilder);
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.export(Mockito.anyCollection()))
        .thenAnswer(
            invocationOnMock -> {
              exportedMetricContainer.addAll(invocationOnMock.getArgument(0));
              return CompletableResultCode.ofSuccess();
            });
    // mock the get default aggregation and aggregation temporality - they're required for valid
    // metric collection.
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.getDefaultAggregation(Mockito.any()))
        .thenAnswer(
            (Answer<Aggregation>)
                invocationOnMock -> {
                  InstrumentType instrumentType = invocationOnMock.getArgument(0);
                  return OtlpGrpcMetricExporter.getDefault().getDefaultAggregation(instrumentType);
                });
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.getAggregationTemporality(Mockito.any()))
        .thenAnswer(
            (Answer<AggregationTemporality>)
                invocationOnMock -> {
                  InstrumentType instrumentType = invocationOnMock.getArgument(0);
                  return OtlpGrpcMetricExporter.getDefault()
                      .getAggregationTemporality(instrumentType);
                });
    Mockito.lenient()
        .when(mockOtlpGrpcMetricExporter.getMemoryMode())
        .thenReturn(MemoryMode.IMMUTABLE_DATA);
  }

  @AutoValue
  abstract static class ProjectIdTestBehavior {
    // A null user specified project ID represents the use case where user omits specifying it
    @Nullable
    abstract String getUserSpecifiedProjectId();

    // The project ID that credentials.getProjectId() returns (can be null)
    @Nullable
    abstract String getCredentialsProjectId();

    // The expected project ID in the resource attributes (null if exception expected)
    @Nullable
    abstract String getExpectedProjectIdInResource();

    // Whether an exception is expected to be thrown
    abstract boolean getExpectedToThrow();

    static Builder builder() {
      return new AutoValue_GcpAuthAutoConfigurationCustomizerProviderTest_ProjectIdTestBehavior
          .Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUserSpecifiedProjectId(String projectId);

      abstract Builder setCredentialsProjectId(String projectId);

      abstract Builder setExpectedProjectIdInResource(String projectId);

      abstract Builder setExpectedToThrow(boolean expectedToThrow);

      abstract ProjectIdTestBehavior build();
    }
  }

  @AutoValue
  abstract static class QuotaProjectIdTestBehavior {
    // A null user specified quota represents the use case where user omits specifying quota
    @Nullable
    abstract String getUserSpecifiedQuotaProjectId();

    abstract boolean getIsQuotaProjectPresentInMetadata();

    // If expected quota project in header is null, the header entry should not be present in export
    @Nullable
    abstract String getExpectedQuotaProjectInHeader();

    static Builder builder() {
      return new AutoValue_GcpAuthAutoConfigurationCustomizerProviderTest_QuotaProjectIdTestBehavior
          .Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUserSpecifiedQuotaProjectId(String quotaProjectId);

      abstract Builder setIsQuotaProjectPresentInMetadata(boolean quotaProjectPresentInMetadata);

      /**
       * Sets the expected quota project header value for the test case. A null value is allowed,
       * and it indicates that the header should not be present in the export request.
       *
       * @param expectedQuotaProjectInHeader the expected header value to match in the export
       *     headers.
       */
      abstract Builder setExpectedQuotaProjectInHeader(String expectedQuotaProjectInHeader);

      abstract QuotaProjectIdTestBehavior build();
    }
  }

  @AutoValue
  abstract static class TargetSignalBehavior {
    @Nonnull
    abstract String getConfiguredTargetSignals();

    @Nonnull
    abstract ImmutableMap<String, String> getUserSpecifiedOtelProperties();

    abstract boolean getExpectedIsTraceSignalModified();

    abstract boolean getExpectedIsMetricsSignalModified();

    static Builder builder() {
      return new AutoValue_GcpAuthAutoConfigurationCustomizerProviderTest_TargetSignalBehavior
          .Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfiguredTargetSignals(String targetSignals);

      abstract Builder setUserSpecifiedOtelProperties(Map<String, String> oTelProperties);

      // Set whether the combination of specified OTel properties and configured target signals
      // should lead to modification of the OTLP trace exporters.
      abstract Builder setExpectedIsTraceSignalModified(boolean expectedModified);

      // Set whether the combination of specified OTel properties and configured target signals
      // should lead to modification of the OTLP metrics exporters.
      abstract Builder setExpectedIsMetricsSignalModified(boolean expectedModified);

      abstract TargetSignalBehavior build();
    }
  }

  // Mockito.lenient is used here because this method is used with parameterized tests where based
  @SuppressWarnings("CannotMockMethod")
  private void prepareMockBehaviorForGoogleCredentials() {
    AccessToken fakeAccessToken = new AccessToken("fake", Date.from(Instant.now()));
    try {
      Mockito.lenient()
          .when(mockedGoogleCredentials.getRequestMetadata())
          .thenReturn(
              ImmutableMap.of(
                  "Authorization",
                  Collections.singletonList("Bearer " + fakeAccessToken.getTokenValue()),
                  QUOTA_USER_PROJECT_HEADER,
                  Collections.singletonList(DUMMY_GCP_QUOTA_PROJECT_ID)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OpenTelemetrySdk buildOpenTelemetrySdkWithExporter(SpanExporter spanExporter) {
    return buildOpenTelemetrySdkWithExporter(
        spanExporter, OtlpHttpMetricExporter.getDefault(), DEFAULT_OTEL_PROPERTIES_SPAN_EXPORTER);
  }

  @SuppressWarnings("UnusedMethod")
  private OpenTelemetrySdk buildOpenTelemetrySdkWithExporter(
      SpanExporter spanExporter, ImmutableMap<String, String> customOTelProperties) {
    return buildOpenTelemetrySdkWithExporter(
        spanExporter, OtlpHttpMetricExporter.getDefault(), customOTelProperties);
  }

  private OpenTelemetrySdk buildOpenTelemetrySdkWithExporter(MetricExporter metricExporter) {
    return buildOpenTelemetrySdkWithExporter(
        OtlpHttpSpanExporter.getDefault(), metricExporter, DEFAULT_OTEL_PROPERTIES_METRIC_EXPORTER);
  }

  @SuppressWarnings("UnusedMethod")
  private OpenTelemetrySdk buildOpenTelemetrySdkWithExporter(
      MetricExporter metricExporter, ImmutableMap<String, String> customOtelProperties) {
    return buildOpenTelemetrySdkWithExporter(
        OtlpHttpSpanExporter.getDefault(), metricExporter, customOtelProperties);
  }

  private OpenTelemetrySdk buildOpenTelemetrySdkWithExporter(
      SpanExporter spanExporter,
      MetricExporter metricExporter,
      ImmutableMap<String, String> customOtelProperties) {
    SpiHelper spiHelper =
        SpiHelper.create(GcpAuthAutoConfigurationCustomizerProviderTest.class.getClassLoader());
    AutoConfiguredOpenTelemetrySdkBuilder builder =
        AutoConfiguredOpenTelemetrySdk.builder()
            .addPropertiesSupplier(() -> customOtelProperties)
            .setComponentLoader(
                new ComponentLoader() {
                  @Override
                  public <T> List<T> load(Class<T> spiClass) {
                    if (spiClass == ConfigurableSpanExporterProvider.class) {
                      return Collections.singletonList(
                          spiClass.cast(
                              new ConfigurableSpanExporterProvider() {
                                @Override
                                public SpanExporter createExporter(
                                    ConfigProperties configProperties) {
                                  return spanExporter;
                                }

                                @Override
                                public String getName() {
                                  return "otlp";
                                }
                              }));
                    }
                    if (spiClass == ConfigurableMetricExporterProvider.class) {
                      return Collections.singletonList(
                          spiClass.cast(
                              new ConfigurableMetricExporterProvider() {
                                @Override
                                public MetricExporter createExporter(
                                    ConfigProperties configProperties) {
                                  return metricExporter;
                                }

                                @Override
                                public String getName() {
                                  return "otlp";
                                }
                              }));
                    }
                    return spiHelper.load(spiClass);
                  }
                });

    return builder.build().getOpenTelemetrySdk();
  }

  private static boolean authHeadersQuotaProjectIsPresent(Map<String, String> headers) {
    Set<Entry<String, String>> headerEntrySet = headers.entrySet();
    return headerEntrySet.contains(
            new SimpleEntry<>(
                QUOTA_USER_PROJECT_HEADER,
                GcpAuthAutoConfigurationCustomizerProviderTest.DUMMY_GCP_QUOTA_PROJECT_ID))
        && headerEntrySet.contains(new SimpleEntry<>("Authorization", "Bearer fake"));
  }

  private static void generateTestSpan(OpenTelemetrySdk openTelemetrySdk) {
    Span span = openTelemetrySdk.getTracer("test").spanBuilder("sample").startSpan();
    try (Scope ignored = span.makeCurrent()) {
      long workOutput = busyloop();
      span.setAttribute("work_loop", workOutput);
    } finally {
      span.end();
    }
  }

  private static void generateTestMetric(OpenTelemetrySdk openTelemetrySdk) {
    LongCounter longCounter =
        openTelemetrySdk
            .getMeter("test")
            .counterBuilder("sample")
            .setDescription("sample counter")
            .setUnit("1")
            .build();
    long workOutput = busyloop();
    long randomValue = TEST_RANDOM.nextInt(1000);
    longCounter.add(randomValue, Attributes.of(AttributeKey.longKey("work_loop"), workOutput));
  }

  // loop to simulate work done
  private static long busyloop() {
    Instant start = Instant.now();
    Instant end;
    long counter = 0;
    do {
      counter++;
      end = Instant.now();
    } while (Duration.between(start, end).toMillis() < 1000);
    return counter;
  }
}
