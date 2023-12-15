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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

@RunWith(JUnit4.class)
public class MetricsToMetricValueConverterTest {

  public static class TestBucketType implements HistogramData.BucketType {
    @Override
    public double getRangeFrom() {return 0.0;}

    @Override
    public double getRangeTo() {return 5.0;}

    @Override
    public int getNumBuckets() {return 1;}

    @Override
    public int getBucketIndex(double value) {return 0;}

    @Override
    public double getBucketSize(int index) {return 5.0;}

    @Override
    public double getAccumulatedBucketSize(int endIndex) {return 5.0;}
  }

  @Test
  public void testConvertCountersToMetricValueUpdates_successfulConvert() {
    Map<MetricName, Long> counters = new HashMap<MetricName, Long>();
    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "baseLabel");
    MetricName bigQueryMetric2 =
        MetricName.named("BigQuerySink", "baseLabel-label1:val1;label2:val2;");
    MetricName bigQueryMetric3 = MetricName.named("BigQuerySink", "zeroValue");

    counters.put(bigQueryMetric1, 5L);
    counters.put(bigQueryMetric2, 10L);
    counters.put(bigQueryMetric3, 0L);

    List<Windmill.MetricValue> values =
        MetricsToMetricValueConverter.convertCountersToMetricValueUpdates(counters);
    Windmill.MetricValue expectedVal1 =
        Windmill.MetricValue.newBuilder().setMetricName("baseLabel").setValueInt64(5L).build();
    Windmill.MetricValue expectedVal2 =
        Windmill.MetricValue.newBuilder()
            .setMetricName("baseLabel")
            .setValueInt64(10L)
            .putMetricLabels("label1", "val1")
            .putMetricLabels("label2", "val2")
            .build();
    assertThat(values.size(), equalTo(2));
    assertThat(values, hasItems(expectedVal1, expectedVal2));
  }

  @Test
  public void testConvertCountersToMetricValueUpdates_skipEmptyMetricName() {
    Map<MetricName, Long> inputCounters = new HashMap<MetricName, Long>();
    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "invalid-metric-name");
    inputCounters.put(bigQueryMetric1, 5L);

    List<Windmill.MetricValue> actualMetricValues =
        MetricsToMetricValueConverter.convertCountersToMetricValueUpdates(inputCounters);
    assertThat(actualMetricValues.size(), equalTo(0));
  }

  @Test
  public void testConvertHistogramsToMetricValueUpdates_successfulConvert() {
    Map<MetricName, HistogramData> inputHistograms = new HashMap<MetricName, HistogramData>();
    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "baseLabel");
    MetricName bigQueryMetric2 =
        MetricName.named("BigQuerySink", "baseLabel-label1:val1;label2:val2;");
    MetricName bigQueryMetric3 = MetricName.named("BigQuerySink", "zeroValue");

    HistogramData nonEmptyLinearHistogram = HistogramData.linear(0, 10, 10);
    nonEmptyLinearHistogram.record(-5.0, 15.0, 25.0, 35.0, 105.0);
    inputHistograms.put(bigQueryMetric1, nonEmptyLinearHistogram);

    HistogramData noEmptyExponentialHistogram = HistogramData.exponential(0, 5);
    noEmptyExponentialHistogram.record(-5.0, 15.0, 25.0, 35.0, 105.0);
    inputHistograms.put(bigQueryMetric2, noEmptyExponentialHistogram);

    HistogramData emptyHistogram = HistogramData.linear(0, 10, 10);
    inputHistograms.put(bigQueryMetric3, emptyHistogram);

    List<Windmill.MetricValue> actualMetricValues =
        MetricsToMetricValueConverter.convertHistogramsToMetricValueUpdates(inputHistograms);

    // Expected value 1
    List<Long> linearHistogramBucketValues = ImmutableList.of(1L, 0L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 1L);
    Windmill.Histogram.Builder linearHistogramBuilder = Windmill.Histogram.newBuilder().setCount(5L).setMean(35.0).setSumOfSquaredDeviations(7000.0).addAllBucketCounts(linearHistogramBucketValues);
    linearHistogramBuilder.getBucketOptionsBuilder().getLinearBuilder().setNumberOfBuckets(12).setWidth(10.0).setStart(0.0);
    Windmill.MetricValue expectedVal1 =
        Windmill.MetricValue.newBuilder().setMetricName("baseLabel").setValueHistogram(linearHistogramBuilder.build()).build();

    // Expected value 2
    List<Long> exponentialHistogramBucketValues = ImmutableList.of(1L, 0L, 0L, 0L, 1L, 1L, 2L);
    Windmill.Histogram.Builder exponentialHistogramBuilder = Windmill.Histogram.newBuilder().setCount(5L).setMean(35.0).setSumOfSquaredDeviations(7000.0).addAllBucketCounts(exponentialHistogramBucketValues);
    exponentialHistogramBuilder.getBucketOptionsBuilder().getExponentialBuilder().setNumberOfBuckets(7).setScale(0);
    Windmill.MetricValue expectedVal2 =
        Windmill.MetricValue.newBuilder()
            .setMetricName("baseLabel")
            .setValueHistogram(exponentialHistogramBuilder.build())
            .putMetricLabels("label1", "val1")
            .putMetricLabels("label2", "val2")
            .build();

    assertThat(actualMetricValues.size(), equalTo(2));
    assertThat(actualMetricValues, hasItems(expectedVal1, expectedVal2));
  }

  @Test
  public void testConvertHistogramsToMetricValueUpdates_skipEmptyMetricName() {
    Map<MetricName, HistogramData> inputHistograms = new HashMap<MetricName, HistogramData>();

    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "invalid-metric-name");
    HistogramData nonEmptyLinearHistogram = HistogramData.linear(0, 10, 10);
    nonEmptyLinearHistogram.record(-5.0, 15.0, 25.0, 35.0, 105.0);
    inputHistograms.put(bigQueryMetric1, nonEmptyLinearHistogram);

    List<Windmill.MetricValue> actualMetricValues =
        MetricsToMetricValueConverter.convertHistogramsToMetricValueUpdates(inputHistograms);
    assertThat(actualMetricValues.size(), equalTo(0));
  }

  @Test
  public void testConvertHistogramsToMetricValueUpdates_skipUnknownBucketType() {
    Map<MetricName, HistogramData> inputHistograms = new HashMap<MetricName, HistogramData>();

    HistogramData histogram = new HistogramData(new TestBucketType());
    histogram.record(1.0, 2.0);
    MetricName bigQueryMetric1 = MetricName.named("BigQuerySink", "baseLabel");
    inputHistograms.put(bigQueryMetric1, histogram);

    List<Windmill.MetricValue> actualMetricValues =
        MetricsToMetricValueConverter.convertHistogramsToMetricValueUpdates(inputHistograms);
    assertThat(actualMetricValues.size(), equalTo(0));
  }
}
