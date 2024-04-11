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
package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Util class for building/parsing labeled {@link MetricName}. */
public class LabeledMetricNameUtils {
  // Delimiters. Avoid using delimeters that can be used in metric labels.
  // E.g. Since we support BigQuery TableIds as labels we avoid using delimiters that can be used in
  // TableIds.
  // ref: https://cloud.google.com/bigquery/docs/tables#table_naming
  private static final char LABEL_DELIMITER = ';';
  private static final char METRIC_KV_DELIMITER = ':';
  private static final char METRIC_NAME_DELIMITER = '*';

  /**
   * Builder class for a labeled {@code MetricName}. Returned {@link MetricName#getName} will be
   * formatted as:
   *
   * <p>'{baseName}-{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
   */
  public static class MetricNameBuilder {
    private final StringBuilder labeledNameBuilder;

    private MetricNameBuilder(String baseName) {
      this.labeledNameBuilder = new StringBuilder(baseName + METRIC_NAME_DELIMITER);
    }

    public static MetricNameBuilder baseNameBuilder(String baseName) {
      return new MetricNameBuilder(baseName);
    }

    /**
     * Add a metric label KV pair to the metric name. Calling {@code addLabel} multiple times with
     * the same {@code key} will lead to unexpected results when parsing metric names.
     */
    public void addLabel(String key, String value) {
      this.labeledNameBuilder
          .append(key)
          .append(METRIC_KV_DELIMITER)
          .append(value)
          .append(LABEL_DELIMITER);
    }

    public MetricName build(String metricNamespace) {
      return MetricName.named(metricNamespace, labeledNameBuilder.toString());
    }
  }

  @AutoValue
  public abstract static class ParsedMetricName {
    public abstract String getBaseName();

    public abstract Map<String, String> getMetricLabels();

    public static ParsedMetricName create(String baseName, Map<String, String> metricLabels) {
      return new AutoValue_LabeledMetricNameUtils_ParsedMetricName(baseName, metricLabels);
    }

    public static ParsedMetricName create(String baseName) {
      ImmutableMap<String, String> emptyMap = ImmutableMap.of();
      return new AutoValue_LabeledMetricNameUtils_ParsedMetricName(baseName, emptyMap);
    }
  }

  /**
   * Parse a 'metric name' String that was created with 'MetricNameBuilder'. The input string should
   * be formatted as.
   *
   * <p>'{baseName}*{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
   *
   * @param metricName
   * @return Returns a ParsedMetricName object if the input string is properly formatted. If the
   *     input string is empty or malformed, returns an empty value.
   */
  public static Optional<ParsedMetricName> parseMetricName(String metricName) {
    if (metricName.isEmpty()) {
      return Optional.empty();
    }

    List<String> metricNameSplit =
        Splitter.on(METRIC_NAME_DELIMITER).limit(2).splitToList(metricName);

    if (metricNameSplit.size() == 0 || metricNameSplit.get(0).isEmpty()) {
      return Optional.empty();
    }

    if (metricNameSplit.size() == 1) {
      return Optional.of(ParsedMetricName.create(metricNameSplit.get(0)));
    }

    Splitter.MapSplitter splitter =
        Splitter.on(LABEL_DELIMITER).omitEmptyStrings().withKeyValueSeparator(METRIC_KV_DELIMITER);
    try {
      Map<String, String> labels = splitter.split(metricNameSplit.get(1));
      return Optional.of(ParsedMetricName.create(metricNameSplit.get(0), labels));
    } catch (IllegalArgumentException e) {
      return Optional.of(ParsedMetricName.create(metricNameSplit.get(0)));
    }
  }
}
