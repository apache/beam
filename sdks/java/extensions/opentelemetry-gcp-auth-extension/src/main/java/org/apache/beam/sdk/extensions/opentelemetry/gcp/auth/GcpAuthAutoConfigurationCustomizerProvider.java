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

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigurationException;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.opentelemetry.gcp.auth.GoogleAuthException.Reason;

/**
 * An AutoConfigurationCustomizerProvider for Google Cloud Platform (GCP) OpenTelemetry (OTLP)
 * integration.
 *
 * <p>This class is registered as a service provider using {@link AutoService} and is responsible
 * for customizing the OpenTelemetry configuration for GCP specific behavior. It retrieves Google
 * Application Default Credentials (ADC) and adds them as authorization headers to the configured
 * {@link SpanExporter}. It also sets default properties and resource attributes for GCP
 * integration.
 *
 * <p>Copied from
 * https://github.com/open-telemetry/opentelemetry-java-contrib/blob/main/gcp-auth-extension/src/main/java/io/opentelemetry/contrib/gcp/auth/GcpAuthAutoConfigurationCustomizerProvider.java
 *
 * @see AutoConfigurationCustomizerProvider
 * @see GoogleCredentials
 */
@Internal
@AutoService(AutoConfigurationCustomizerProvider.class)
public class GcpAuthAutoConfigurationCustomizerProvider
    implements AutoConfigurationCustomizerProvider {

  private static final Logger LOG =
      Logger.getLogger(GcpAuthAutoConfigurationCustomizerProvider.class.getName());
  private static final String SIGNAL_TARGET_WARNING_FIX_SUGGESTION =
      String.format(
          "You may safely ignore this warning if it is intentional, otherwise please configure the '%s' by exporting valid values to environment variable: %s or by setting valid values in system property: %s.",
          ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getUserReadableName(),
          ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getEnvironmentVariable(),
          ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getSystemProperty());

  static final String QUOTA_USER_PROJECT_HEADER = "x-goog-user-project";
  static final String GCP_USER_PROJECT_ID_KEY = "gcp.project_id";

  static final String SIGNAL_TYPE_TRACES = "traces";
  static final String SIGNAL_TYPE_METRICS = "metrics";
  static final String SIGNAL_TYPE_ALL = "all";
  static final String SIGNAL_TYPE_NONE = "none";

  /**
   * Customizes the provided {@link AutoConfigurationCustomizer} such that authenticated exports to
   * GCP Telemetry API are possible from the configured OTLP exporter.
   *
   * <p>This method attempts to retrieve Google Application Default Credentials (ADC) and performs
   * the following:
   *
   * <ul>
   *   <li>Verifies whether the configured OTLP endpoint (base or signal specific) is a known GCP
   *       endpoint.
   *   <li>If the configured base OTLP endpoint is a known GCP Telemetry API endpoint, customizes
   *       both the configured OTLP {@link SpanExporter} and {@link MetricExporter}.
   *   <li>If the configured signal specific endpoint is a known GCP Telemetry API endpoint,
   *       customizes only the signal specific exporter.
   * </ul>
   *
   * The 'customization' performed includes customizing the exporters by adding required headers to
   * the export calls made and customizing the resource by adding required resource attributes to
   * enable GCP integration.
   *
   * @param autoConfiguration the AutoConfigurationCustomizer to customize.
   */
  @Override
  public void customize(@Nonnull AutoConfigurationCustomizer autoConfiguration) {
    java.util.function.Supplier<GoogleCredentials> credentialsSupplier =
        new java.util.function.Supplier<GoogleCredentials>() {
          private @javax.annotation.Nullable GoogleCredentials credentials;

          @Override
          public synchronized GoogleCredentials get() {
            if (credentials == null) {
              try {
                credentials = GoogleCredentials.getApplicationDefault();
              } catch (IOException e) {
                throw new GoogleAuthException(Reason.FAILED_ADC_RETRIEVAL, e);
              }
            }
            return credentials;
          }
        };
    autoConfiguration
        .addSpanExporterCustomizer(
            (spanExporter, configProperties) ->
                customizeSpanExporter(spanExporter, credentialsSupplier, configProperties))
        .addMetricExporterCustomizer(
            (metricExporter, configProperties) ->
                customizeMetricExporter(metricExporter, credentialsSupplier, configProperties))
        .addResourceCustomizer(
            (resource, configProperties) ->
                customizeResource(resource, credentialsSupplier, configProperties));
  }

  @Override
  public int order() {
    return Integer.MAX_VALUE - 1;
  }

  private static SpanExporter customizeSpanExporter(
      SpanExporter exporter,
      java.util.function.Supplier<GoogleCredentials> credentialsSupplier,
      ConfigProperties configProperties) {
    if (isSignalTargeted(SIGNAL_TYPE_TRACES, configProperties)) {
      return addAuthorizationHeaders(exporter, credentialsSupplier.get(), configProperties);
    } else {
      String[] params = {
        SIGNAL_TYPE_TRACES, SIGNAL_TYPE_NONE, SIGNAL_TARGET_WARNING_FIX_SUGGESTION
      };
      LOG.log(
          Level.WARNING,
          "GCP Authentication Extension is not configured for signal type: {0} or is configured with signal type: {1}. {2}",
          params);
    }
    return exporter;
  }

  private static MetricExporter customizeMetricExporter(
      MetricExporter exporter,
      java.util.function.Supplier<GoogleCredentials> credentialsSupplier,
      ConfigProperties configProperties) {
    if (isSignalTargeted(SIGNAL_TYPE_METRICS, configProperties)) {
      return addAuthorizationHeaders(exporter, credentialsSupplier.get(), configProperties);
    } else {
      String[] params = {
        SIGNAL_TYPE_METRICS, SIGNAL_TYPE_NONE, SIGNAL_TARGET_WARNING_FIX_SUGGESTION
      };
      LOG.log(
          Level.WARNING,
          "GCP Authentication Extension is not configured for signal type: {0} or is configured with signal type: {1}. {2}",
          params);
    }
    return exporter;
  }

  // Checks if the auth extension is configured to target the passed signal for authentication.
  private static boolean isSignalTargeted(String checkSignal, ConfigProperties configProperties) {
    String userSpecifiedTargetedSignals =
        ConfigurableOption.GOOGLE_OTEL_AUTH_TARGET_SIGNALS.getConfiguredValueWithFallback(
            configProperties, () -> SIGNAL_TYPE_ALL);
    List<String> targetedSignals =
        stream(userSpecifiedTargetedSignals.split(",")).map(String::trim).collect(toList());
    if (targetedSignals.contains(SIGNAL_TYPE_NONE)) {
      return false;
    }
    return targetedSignals.contains(checkSignal) || targetedSignals.contains(SIGNAL_TYPE_ALL);
  }

  // Adds authorization headers to the calls made by the OtlpGrpcSpanExporter and
  // OtlpHttpSpanExporter.
  private static SpanExporter addAuthorizationHeaders(
      SpanExporter exporter, GoogleCredentials credentials, ConfigProperties configProperties) {
    if (exporter instanceof OtlpHttpSpanExporter) {
      OtlpHttpSpanExporterBuilder builder =
          ((OtlpHttpSpanExporter) exporter)
              .toBuilder()
              .setHeaders(() -> getRequiredHeaderMap(credentials, configProperties));
      return builder.build();
    } else if (exporter instanceof OtlpGrpcSpanExporter) {
      OtlpGrpcSpanExporterBuilder builder =
          ((OtlpGrpcSpanExporter) exporter)
              .toBuilder()
              .setHeaders(() -> getRequiredHeaderMap(credentials, configProperties));
      return builder.build();
    }
    return exporter;
  }

  // Adds authorization headers to the calls made by the OtlpGrpcMetricExporter and
  // OtlpHttpMetricExporter.
  private static MetricExporter addAuthorizationHeaders(
      MetricExporter exporter, GoogleCredentials credentials, ConfigProperties configProperties) {
    if (exporter instanceof OtlpHttpMetricExporter) {
      OtlpHttpMetricExporterBuilder builder =
          ((OtlpHttpMetricExporter) exporter)
              .toBuilder()
              .setHeaders(() -> getRequiredHeaderMap(credentials, configProperties));
      return builder.build();
    } else if (exporter instanceof OtlpGrpcMetricExporter) {
      OtlpGrpcMetricExporterBuilder builder =
          ((OtlpGrpcMetricExporter) exporter)
              .toBuilder()
              .setHeaders(() -> getRequiredHeaderMap(credentials, configProperties));
      return builder.build();
    }
    return exporter;
  }

  private static Map<String, String> getRequiredHeaderMap(
      GoogleCredentials credentials, ConfigProperties configProperties) {
    Map<String, List<String>> gcpHeaders;
    try {
      // this also refreshes the credentials, if required
      gcpHeaders = credentials.getRequestMetadata();
    } catch (IOException e) {
      throw new GoogleAuthException(Reason.FAILED_ADC_REFRESH, e);
    }
    Map<String, String> flattenedHeaders =
        gcpHeaders.entrySet().stream()
            .collect(
                toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .filter(Objects::nonNull) // Filter nulls
                            .filter(s -> !s.isEmpty()) // Filter empty strings
                            .collect(joining(","))));
    // Add quota user project header if not detected by the auth library and user provided it via
    // system properties.
    if (!flattenedHeaders.containsKey(QUOTA_USER_PROJECT_HEADER)) {
      Optional<String> maybeConfiguredQuotaProjectId =
          ConfigurableOption.GOOGLE_CLOUD_QUOTA_PROJECT.getConfiguredValueAsOptional(
              configProperties);
      maybeConfiguredQuotaProjectId.ifPresent(
          configuredQuotaProjectId ->
              flattenedHeaders.put(QUOTA_USER_PROJECT_HEADER, configuredQuotaProjectId));
    }
    return flattenedHeaders;
  }

  // Updates the current resource with the attributes required for ingesting OTLP data on GCP.
  private static Resource customizeResource(
      Resource resource,
      java.util.function.Supplier<GoogleCredentials> credentialsSupplier,
      ConfigProperties configProperties) {
    if (!isSignalTargeted(SIGNAL_TYPE_TRACES, configProperties)
        && !isSignalTargeted(SIGNAL_TYPE_METRICS, configProperties)) {
      return resource;
    }
    String gcpProjectId;
    try {
      gcpProjectId = ConfigurableOption.GOOGLE_CLOUD_PROJECT.getConfiguredValue(configProperties);
    } catch (ConfigurationException e) {
      gcpProjectId = credentialsSupplier.get().getProjectId();
      if (gcpProjectId == null || gcpProjectId.isEmpty()) {
        throw e;
      }
    }
    Resource res = Resource.create(Attributes.of(stringKey(GCP_USER_PROJECT_ID_KEY), gcpProjectId));
    return resource.merge(res);
  }
}
