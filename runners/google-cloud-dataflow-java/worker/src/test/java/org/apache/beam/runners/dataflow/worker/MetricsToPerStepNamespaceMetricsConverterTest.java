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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.google.api.services.dataflow.model.Base2Exponent;
import com.google.api.services.dataflow.model.BucketOptions;
import com.google.api.services.dataflow.model.DataflowHistogramValue;
import com.google.api.services.dataflow.model.Linear;
import com.google.api.services.dataflow.model.MetricValue;
import com.google.api.services.dataflow.model.OutlierStats;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.LabeledMetricNameUtils;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.ImmutableLongArray;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetricsToPerStepNamespaceMetricsConverterTest {
  private static final HistogramData.BucketType lienarBuckets =
      HistogramData.LinearBuckets.of(0, 10, 10);
  private static final HistogramData.BucketType exponentialBuckets =
      HistogramData.ExponentialBuckets.of(0, 5);

  public static class TestBucketType implements HistogramData.BucketType {
    @Override
    public double getRangeFrom() {
      return 0.0;
    }

    @Override
    public double getRangeTo() {
      return 5.0;
    }

    @Override
    public int getNumBuckets() {
      return 1;
    }

    @Override
    public int getBucketIndex(double value) {
      return 0;
    }

    @Override
    public double getBucketSize(int index) {
      return 5.0;
    }

    @Override
    public double getAccumulatedBucketSize(int endIndex) {
      return 5.0;
    }
  }

  @Test
  public void testConvert_successfulyConvertCounters() {
    String step = "testStepName";
    Map<MetricName, LockFreeHistogram.Snapshot> emptyHistograms = new HashMap<>();
    Map<MetricName, Long> counters = new HashMap<MetricName, Long>();
    Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNames = new HashMap<>();

    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "metric1");
    MetricName bigQueryMetric2 =
        MetricName.named("BigQuerySink", "metric2*label1:val1;label2:val2;");
    MetricName bigQueryMetric3 = MetricName.named("BigQuerySink", "zeroValue");

    counters.put(bigQueryMetric1, 5L);
    counters.put(bigQueryMetric2, 10L);
    counters.put(bigQueryMetric3, 0L);

    Collection<PerStepNamespaceMetrics> conversionResult =
        MetricsToPerStepNamespaceMetricsConverter.convert(
            step, counters, emptyHistograms, parsedMetricNames);

    MetricValue expectedVal1 =
        new MetricValue().setMetric("metric1").setValueInt64(5L).setMetricLabels(new HashMap<>());
    Map<String, String> val2LabelMap = new HashMap<>();
    val2LabelMap.put("label1", "val1");
    val2LabelMap.put("label2", "val2");
    MetricValue expectedVal2 =
        new MetricValue().setMetric("metric2").setValueInt64(10L).setMetricLabels(val2LabelMap);

    assertThat(conversionResult.size(), equalTo(1));
    PerStepNamespaceMetrics perStepNamespaceMetrics = conversionResult.iterator().next();

    assertThat(perStepNamespaceMetrics.getOriginalStep(), equalTo(step));
    assertThat(perStepNamespaceMetrics.getMetricsNamespace(), equalTo("BigQuerySink"));
    assertThat(perStepNamespaceMetrics.getMetricValues().size(), equalTo(2));
    assertThat(
        perStepNamespaceMetrics.getMetricValues(), containsInAnyOrder(expectedVal1, expectedVal2));

    LabeledMetricNameUtils.ParsedMetricName parsedBigQueryMetric1 =
        LabeledMetricNameUtils.parseMetricName(bigQueryMetric1.getName()).get();
    LabeledMetricNameUtils.ParsedMetricName parsedBigQueryMetric2 =
        LabeledMetricNameUtils.parseMetricName(bigQueryMetric2.getName()).get();

    assertThat(parsedMetricNames.size(), equalTo(2));
    assertThat(parsedMetricNames, IsMapContaining.hasEntry(bigQueryMetric1, parsedBigQueryMetric1));
    assertThat(parsedMetricNames, IsMapContaining.hasEntry(bigQueryMetric2, parsedBigQueryMetric2));
  }

  @Test
  public void testConvert_skipInvalidMetricNames() {
    Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNames = new HashMap<>();

    Map<MetricName, Long> counters = new HashMap<>();
    MetricName invalidName1 = MetricName.named("BigQuerySink", "**");
    counters.put(invalidName1, 5L);

    Map<MetricName, LockFreeHistogram.Snapshot> histograms = new HashMap<>();
    MetricName invalidName2 = MetricName.named("BigQuerySink", "****");
    LockFreeHistogram nonEmptyLinearHistogram = new LockFreeHistogram(invalidName2, lienarBuckets);
    nonEmptyLinearHistogram.update(-5.0);
    histograms.put(invalidName2, nonEmptyLinearHistogram.getSnapshotAndReset().get());

    Collection<PerStepNamespaceMetrics> conversionResult =
        MetricsToPerStepNamespaceMetricsConverter.convert(
            "testStep", counters, histograms, parsedMetricNames);
    assertThat(conversionResult.size(), equalTo(0));
    assertThat(parsedMetricNames.size(), equalTo(0));
  }

  @Test
  public void testConvert_successfulConvertHistograms() {
    Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNames = new HashMap<>();

    Map<MetricName, LockFreeHistogram.Snapshot> histograms = new HashMap<>();
    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "baseLabel");
    MetricName bigQueryMetric2 =
        MetricName.named("BigQuerySink", "baseLabel*label1:val1;label2:val2;");
    MetricName bigQueryMetric3 = MetricName.named("BigQuerySink", "zeroValue");

    LockFreeHistogram nonEmptyLinearHistogram =
        new LockFreeHistogram(bigQueryMetric1, lienarBuckets);
    nonEmptyLinearHistogram.update(-5.0, 15.0, 25.0, 35.0, 105.0);
    histograms.put(bigQueryMetric1, nonEmptyLinearHistogram.getSnapshotAndReset().get());

    LockFreeHistogram noEmptyExponentialHistogram =
        new LockFreeHistogram(bigQueryMetric2, exponentialBuckets);
    noEmptyExponentialHistogram.update(-5.0, 15.0, 25.0, 35.0, 105.0);
    histograms.put(bigQueryMetric2, noEmptyExponentialHistogram.getSnapshotAndReset().get());

    LockFreeHistogram.Snapshot emptySnapshot =
        LockFreeHistogram.Snapshot.create(
            LockFreeHistogram.OutlierStatistic.EMPTY,
            LockFreeHistogram.OutlierStatistic.EMPTY,
            ImmutableLongArray.of(),
            lienarBuckets);
    histograms.put(bigQueryMetric3, emptySnapshot);

    String step = "testStep";
    Map<MetricName, Long> emptyCounters = new HashMap<>();
    Collection<PerStepNamespaceMetrics> conversionResult =
        MetricsToPerStepNamespaceMetricsConverter.convert(
            step, emptyCounters, histograms, parsedMetricNames);

    // Expected value 1
    List<Long> bucketCounts1 = ImmutableList.of(0L, 1L, 1L, 1L);

    Linear linearOptions1 = new Linear().setNumberOfBuckets(10).setWidth(10.0).setStart(0.0);
    BucketOptions bucketOptions1 = new BucketOptions().setLinear(linearOptions1);

    OutlierStats outlierStats1 =
        new OutlierStats()
            .setUnderflowCount(1L)
            .setUnderflowMean(-5.0)
            .setOverflowCount(1L)
            .setOverflowMean(105.0);
    DataflowHistogramValue linearHistogram1 =
        new DataflowHistogramValue()
            .setCount(5L)
            .setBucketOptions(bucketOptions1)
            .setBucketCounts(bucketCounts1)
            .setOutlierStats(outlierStats1);

    MetricValue expectedVal1 =
        new MetricValue()
            .setMetric("baseLabel")
            .setMetricLabels(new HashMap<>())
            .setValueHistogram(linearHistogram1);

    // Expected value 2
    List<Long> bucketCounts2 = ImmutableList.of(0L, 0L, 0L, 1L, 1L);
    OutlierStats outlierStats2 =
        new OutlierStats()
            .setUnderflowCount(1L)
            .setUnderflowMean(-5.0)
            .setOverflowCount(2L)
            .setOverflowMean(70.0);
    Base2Exponent exponentialOptions2 = new Base2Exponent().setNumberOfBuckets(5).setScale(0);

    BucketOptions bucketOptions2 = new BucketOptions().setExponential(exponentialOptions2);

    DataflowHistogramValue exponentialHistogram2 =
        new DataflowHistogramValue()
            .setCount(5L)
            .setBucketOptions(bucketOptions2)
            .setBucketCounts(bucketCounts2)
            .setOutlierStats(outlierStats2);

    Map<String, String> metric2Labels = new HashMap<>();
    metric2Labels.put("label1", "val1");
    metric2Labels.put("label2", "val2");
    MetricValue expectedVal2 =
        new MetricValue()
            .setMetric("baseLabel")
            .setValueHistogram(exponentialHistogram2)
            .setMetricLabels(metric2Labels);

    assertThat(conversionResult.size(), equalTo(1));
    PerStepNamespaceMetrics perStepNamespaceMetrics = conversionResult.iterator().next();

    assertThat(perStepNamespaceMetrics.getOriginalStep(), equalTo(step));
    assertThat(perStepNamespaceMetrics.getMetricsNamespace(), equalTo("BigQuerySink"));
    assertThat(perStepNamespaceMetrics.getMetricValues().size(), equalTo(2));
    assertThat(
        perStepNamespaceMetrics.getMetricValues(), containsInAnyOrder(expectedVal1, expectedVal2));

    // Verify that parsedMetricNames have been cached.
    LabeledMetricNameUtils.ParsedMetricName parsedBigQueryMetric1 =
        LabeledMetricNameUtils.parseMetricName(bigQueryMetric1.getName()).get();
    LabeledMetricNameUtils.ParsedMetricName parsedBigQueryMetric2 =
        LabeledMetricNameUtils.parseMetricName(bigQueryMetric2.getName()).get();

    assertThat(parsedMetricNames.size(), equalTo(2));
    assertThat(parsedMetricNames, IsMapContaining.hasEntry(bigQueryMetric1, parsedBigQueryMetric1));
    assertThat(parsedMetricNames, IsMapContaining.hasEntry(bigQueryMetric2, parsedBigQueryMetric2));
  }

  @Test
  public void testConvert_skipUnknownHistogramBucketType() {
    Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNames = new HashMap<>();

    String step = "testStep";
    Map<MetricName, Long> emptyCounters = new HashMap<>();
    Map<MetricName, LockFreeHistogram.Snapshot> histograms = new HashMap<>();

    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "baseLabel");
    LockFreeHistogram histogram = new LockFreeHistogram(bigQueryMetric1, new TestBucketType());
    histogram.update(1.0, 2.0);
    histograms.put(bigQueryMetric1, histogram.getSnapshotAndReset().get());

    Collection<PerStepNamespaceMetrics> conversionResult =
        MetricsToPerStepNamespaceMetricsConverter.convert(
            step, emptyCounters, histograms, parsedMetricNames);
    assertThat(conversionResult.size(), equalTo(0));
    assertThat(parsedMetricNames.size(), equalTo(0));
  }

  @Test
  public void testConvert_convertCountersAndHistograms() {
    String step = "testStep";
    Map<MetricName, Long> counters = new HashMap<>();
    Map<MetricName, LockFreeHistogram.Snapshot> histograms = new HashMap<>();
    Map<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNames = new HashMap<>();

    MetricName counterMetricName = MetricName.named("BigQuerySink", "counter*label1:val1;");
    counters.put(counterMetricName, 3L);

    MetricName histogramMetricName = MetricName.named("BigQuerySink", "histogram*label2:val2;");
    LockFreeHistogram linearHistogram = new LockFreeHistogram(histogramMetricName, lienarBuckets);
    linearHistogram.update(5.0);
    histograms.put(histogramMetricName, linearHistogram.getSnapshotAndReset().get());

    Collection<PerStepNamespaceMetrics> conversionResult =
        MetricsToPerStepNamespaceMetricsConverter.convert(
            step, counters, histograms, parsedMetricNames);

    // Expected counter MetricValue
    Map<String, String> counterLabelMap = new HashMap<>();
    counterLabelMap.put("label1", "val1");
    MetricValue expectedCounter =
        new MetricValue().setMetric("counter").setValueInt64(3L).setMetricLabels(counterLabelMap);

    // Expected histogram MetricValue
    List<Long> bucketCounts1 = ImmutableList.of(1L);

    Linear linearOptions1 = new Linear().setNumberOfBuckets(10).setWidth(10.0).setStart(0.0);
    BucketOptions bucketOptions1 = new BucketOptions().setLinear(linearOptions1);

    DataflowHistogramValue linearHistogram1 =
        new DataflowHistogramValue()
            .setCount(1L)
            .setBucketOptions(bucketOptions1)
            .setBucketCounts(bucketCounts1);

    Map<String, String> histogramLabelMap = new HashMap<>();
    histogramLabelMap.put("label2", "val2");

    MetricValue expectedHistogram =
        new MetricValue()
            .setMetric("histogram")
            .setMetricLabels(histogramLabelMap)
            .setValueHistogram(linearHistogram1);

    assertThat(conversionResult.size(), equalTo(1));
    PerStepNamespaceMetrics perStepNamespaceMetrics = conversionResult.iterator().next();

    assertThat(perStepNamespaceMetrics.getOriginalStep(), equalTo(step));
    assertThat(perStepNamespaceMetrics.getMetricsNamespace(), equalTo("BigQuerySink"));
    assertThat(perStepNamespaceMetrics.getMetricValues().size(), equalTo(2));
    assertThat(
        perStepNamespaceMetrics.getMetricValues(),
        containsInAnyOrder(expectedCounter, expectedHistogram));

    // Verify that parsedMetricNames have been cached.
    LabeledMetricNameUtils.ParsedMetricName parsedCounterMetricName =
        LabeledMetricNameUtils.parseMetricName(counterMetricName.getName()).get();
    LabeledMetricNameUtils.ParsedMetricName parsedHistogramMetricName =
        LabeledMetricNameUtils.parseMetricName(histogramMetricName.getName()).get();

    assertThat(parsedMetricNames.size(), equalTo(2));
    assertThat(
        parsedMetricNames, IsMapContaining.hasEntry(counterMetricName, parsedCounterMetricName));
    assertThat(
        parsedMetricNames,
        IsMapContaining.hasEntry(histogramMetricName, parsedHistogramMetricName));
  }
}
