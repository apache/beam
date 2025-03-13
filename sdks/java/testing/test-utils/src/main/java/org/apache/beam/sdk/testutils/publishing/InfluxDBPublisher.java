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
package org.apache.beam.sdk.testutils.publishing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfluxDBPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBPublisher.class);

  private InfluxDBPublisher() {}

  /** InfluxDB data point. */
  @AutoValue
  public abstract static class DataPoint {
    DataPoint() {}

    public abstract @Pure String measurement();

    public abstract @Pure Map<String, String> tags();

    public abstract @Pure Map<String, Number> fields();

    @Nullable
    public abstract @Pure Long timestamp();

    public abstract @Pure TimeUnit timestampUnit();

    @Override
    public final String toString() {
      return append(new StringBuilder()).toString();
    }

    private @Nullable Long timestampSecs() {
      return timestamp() != null ? timestampUnit().toSeconds(timestamp()) : null;
    }

    private StringBuilder append(StringBuilder builder) {
      return addMeasurement(builder, measurement(), tags(), fields(), timestampSecs());
    }
  }

  /** Creates an InfluxDB data point using optional custom Unix timestamp in seconds. */
  public static DataPoint dataPoint(
      String measurement,
      Map<String, String> tags,
      Map<String, Number> fields,
      @Nullable Long timestampSecs) {
    Preconditions.checkArgument(isNotBlank(measurement), "Measurement cannot be blank");
    return new AutoValue_InfluxDBPublisher_DataPoint(
        measurement, tags, fields, timestampSecs, TimeUnit.SECONDS);
  }

  /** Creates an InfluxDB data point. */
  public static DataPoint dataPoint(
      String measurement,
      Map<String, String> tags,
      Map<String, Number> fields,
      @Nullable Long timestamp,
      TimeUnit timestampUnit) {
    Preconditions.checkArgument(isNotBlank(measurement), "Measurement cannot be blank");
    return new AutoValue_InfluxDBPublisher_DataPoint(
        measurement, tags, fields, timestamp, timestampUnit);
  }

  /** @deprecated Use {@link #publish} instead. */
  @Deprecated
  public static void publishNexmarkResults(
      final Collection<Map<String, Object>> results,
      final InfluxDBSettings settings,
      final Map<String, String> tags) {
    publishWithCheck(settings, nexmarkDataPoints(results, tags));
  }

  public static void publishWithSettings(
      final Collection<NamedTestResult> results, final InfluxDBSettings settings) {
    if (isNotBlank(settings.measurement)) {
      @SuppressWarnings("nullness")
      Collection<DataPoint> dataPoints =
          Collections2.transform(results, res -> res.toInfluxDBDataPoint(settings.measurement));
      publish(settings, dataPoints);
    } else {
      LOG.warn("Missing setting InfluxDB measurement. Metrics won't be published.");
    }
  }

  public static void publish(
      final InfluxDBSettings settings, final Collection<DataPoint> dataPoints) {
    final StringBuilder builder = new StringBuilder();
    dataPoints.forEach(m -> m.append(builder).append('\n'));
    publishWithCheck(settings, builder.toString());
  }

  private static void publishWithCheck(final InfluxDBSettings settings, final String data) {
    requireNonNull(settings, "InfluxDB settings must not be null");
    if (isNotBlank(settings.database)) {
      try {
        final HttpClientBuilder builder = provideHttpBuilder(settings);
        final HttpPost postRequest = providePOSTRequest(settings);
        postRequest.setEntity(new GzipCompressingEntity(new ByteArrayEntity(data.getBytes(UTF_8))));
        executeWithVerification(postRequest, builder);
      } catch (Exception exception) {
        LOG.warn("Unable to publish metrics due to error: {}", exception.getMessage());
      }
    } else {
      LOG.warn("Missing setting InfluxDB database. Metrics won't be published.");
    }
  }

  /** @deprecated To be removed, kept for legacy interface {@link #publishNexmarkResults} */
  @VisibleForTesting
  @Deprecated
  static String nexmarkDataPoints(
      final Collection<Map<String, Object>> results, final Map<String, String> tags) {
    final StringBuilder builder = new StringBuilder();
    final Set<String> fields = ImmutableSet.of("runtimeMs", "numResults");
    results.forEach(
        map -> {
          String measurement = checkArgumentNotNull(map.get("measurement")).toString();
          addMeasurement(builder, measurement, tags, filterKeys(map, fields), map.get("timestamp"))
              .append('\n');
        });
    return builder.toString();
  }

  @SuppressWarnings("nullness")
  private static <K, V> Map<K, V> filterKeys(final Map<K, V> map, final Set<K> keys) {
    return Maps.filterKeys(map, keys::contains);
  }

  // fix types once nexmarkMeasurements is removed
  private static StringBuilder addMeasurement(
      StringBuilder builder,
      String measurement,
      Map<String, ?> tags,
      Map<String, ?> fields,
      @Nullable Object timestampSecs) {
    checkState(!fields.isEmpty(), "fields cannot be empty");
    builder.append(measurement);
    tags.forEach((k, v) -> builder.append(',').append(k).append('=').append(v));
    builder.append(' ');
    fields.forEach((k, v) -> builder.append(k).append('=').append(fieldValue(v)).append(','));
    builder.setLength(builder.length() - 1); // skip last comma
    if (timestampSecs != null) {
      builder.append(' ').append(timestampSecs);
    }
    return builder;
  }

  private static String fieldValue(@Nullable Object value) {
    checkStateNotNull(value, "field value cannot be null");
    // append 'i' suffix for 64-bit integer, default is float
    return (value instanceof Integer || value instanceof Long) ? value + "i" : value.toString();
  }

  private static HttpClientBuilder provideHttpBuilder(final InfluxDBSettings settings) {
    final HttpClientBuilder builder = HttpClientBuilder.create();

    if (isNoneBlank(settings.userName, settings.userPassword)) {
      final CredentialsProvider provider = new BasicCredentialsProvider();
      provider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(settings.userName, settings.userPassword));
      builder.setDefaultCredentialsProvider(provider);
    }

    return builder;
  }

  private static HttpPost providePOSTRequest(final InfluxDBSettings settings) {
    final String retentionPolicy =
        "rp" + (isBlank(settings.retentionPolicy) ? "" : "=" + settings.retentionPolicy);
    return new HttpPost(
        settings.host + "/write?db=" + settings.database + "&" + retentionPolicy + "&precision=s");
  }

  private static void executeWithVerification(
      final HttpPost postRequest, final HttpClientBuilder builder) throws IOException {
    try (final CloseableHttpResponse response = builder.build().execute(postRequest)) {
      is2xx(response);
    }
  }

  private static void is2xx(final HttpResponse response) throws IOException {
    final int code = response.getStatusLine().getStatusCode();
    if (code < 200 || code >= 300) {
      throw new IOException(
          "Response code: " + code + ". Reason: " + getErrorMessage(response.getEntity()));
    }
  }

  private static String getErrorMessage(final HttpEntity entity) throws IOException {
    final Header encodingHeader = entity.getContentEncoding();
    final Charset encoding =
        encodingHeader == null
            ? StandardCharsets.UTF_8
            : Charsets.toCharset(encodingHeader.getValue());
    final JsonElement errorElement =
        new Gson().fromJson(EntityUtils.toString(entity, encoding), JsonObject.class).get("error");
    return isNull(errorElement) ? "[Unable to get error message]" : errorElement.toString();
  }
}
