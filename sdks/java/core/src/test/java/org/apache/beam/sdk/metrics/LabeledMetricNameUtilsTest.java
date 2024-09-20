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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class LabeledMetricNameUtilsTest implements Serializable {
  @Test
  public void testParseMetricName_noLabels() {
    String baseMetricName = "baseMetricName";
    LabeledMetricNameUtils.MetricNameBuilder builder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(baseMetricName);
    String metricName = builder.build("namespace").getName();
    Optional<LabeledMetricNameUtils.ParsedMetricName> parsedName =
        LabeledMetricNameUtils.parseMetricName(metricName);

    LabeledMetricNameUtils.ParsedMetricName expectedParsedName =
        LabeledMetricNameUtils.ParsedMetricName.create(baseMetricName);

    assertThat(parsedName.isPresent(), equalTo(true));
    assertThat(parsedName.get(), equalTo(expectedParsedName));
    assertThat(parsedName.get().getBaseName(), equalTo(baseMetricName));
  }

  @Test
  public void testParseMetricName_successfulLabels() {
    String baseMetricName = "baseMetricName";
    LabeledMetricNameUtils.MetricNameBuilder builder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(baseMetricName);
    builder.addLabel("key1", "val1");
    builder.addLabel("key2", "val2");
    builder.addLabel("key3", "val3");
    String metricName = builder.build("namespace").getName();
    Optional<LabeledMetricNameUtils.ParsedMetricName> parsedName =
        LabeledMetricNameUtils.parseMetricName(metricName);

    String expectedMetricName = "baseMetricName*key1:val1;key2:val2;key3:val3;";
    ImmutableMap<String, String> expectedLabels =
        ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3");
    LabeledMetricNameUtils.ParsedMetricName expectedParsedName =
        LabeledMetricNameUtils.ParsedMetricName.create(baseMetricName, expectedLabels);

    assertThat(metricName, equalTo(expectedMetricName));
    assertThat(parsedName.isPresent(), equalTo(true));
    assertThat(parsedName.get(), equalTo(expectedParsedName));
    assertThat(parsedName.get().getBaseName(), equalTo(baseMetricName));
    assertThat(parsedName.get().getMetricLabels(), equalTo(expectedLabels));
  }

  @Test
  public void testParseMetricName_malformedMetricLabels() {
    String metricName = "baseLabel*malformed_kv_pair;key2:val2;";
    LabeledMetricNameUtils.ParsedMetricName expectedName =
        LabeledMetricNameUtils.ParsedMetricName.create("baseLabel");

    Optional<LabeledMetricNameUtils.ParsedMetricName> parsedMetricName =
        LabeledMetricNameUtils.parseMetricName(metricName);

    assertThat(parsedMetricName.isPresent(), equalTo(true));
    assertThat(parsedMetricName.get(), equalTo(expectedName));
  }

  @Test
  public void testParseMetricName_emptyString() {
    assertThat(LabeledMetricNameUtils.parseMetricName("").isPresent(), equalTo(false));
  }

  @Test
  public void testParseMetricName_emptyMetric() {
    LabeledMetricNameUtils.MetricNameBuilder builder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder("");
    String metricName = builder.build("namespace").getName();
    assertThat(LabeledMetricNameUtils.parseMetricName(metricName).isPresent(), equalTo(false));
  }
}
