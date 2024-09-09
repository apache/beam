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

import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import com.google.api.services.dataflow.model.BucketOptions;
import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DataflowHistogramValue;
import com.google.api.services.dataflow.model.DistributionUpdate;
import com.google.api.services.dataflow.model.IntegerGauge;
import com.google.api.services.dataflow.model.Linear;
import com.google.api.services.dataflow.model.MetricValue;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import com.google.api.services.dataflow.model.StringList;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Origin;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.LabeledMetricNameUtils;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.NoOpCounter;
import org.apache.beam.sdk.metrics.NoOpHistogram;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.collection.IsMapContaining;
import org.joda.time.DateTimeUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StreamingStepMetricsContainer}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class StreamingStepMetricsContainerTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private MetricsContainerRegistry registry = StreamingStepMetricsContainer.createRegistry();

  private MetricsContainer c1 = registry.getContainer("s1");
  private MetricsContainer c2 = registry.getContainer("s2");

  private MetricName name1 = MetricName.named("ns", "name1");
  private MetricName name2 = MetricName.named("ns", "name2");

  @Test
  public void testDedupping() {
    assertThat(c1, not(sameInstance(c2)));
    assertThat(c1, sameInstance(registry.getContainer("s1")));
  }

  @Test
  public void testCounterUpdateExtraction() {
    c1.getCounter(name1).inc(5);
    c2.getCounter(name1).inc(8);
    c2.getCounter(name2).inc(12);

    Iterable<CounterUpdate> updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s1"))
                        .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
                .setCumulative(false)
                .setInteger(longToSplitInt(5)),
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s2"))
                        .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
                .setCumulative(false)
                .setInteger(longToSplitInt(8)),
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name2")
                                .setOriginalStepName("s2"))
                        .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
                .setCumulative(false)
                .setInteger(longToSplitInt(12))));

    c2.getCounter(name1).inc(7);

    updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s2"))
                        .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
                .setCumulative(false)
                .setInteger(longToSplitInt(7))));
  }

  @Test
  public void testDistributionUpdateExtraction() {
    Distribution distribution = c1.getDistribution(name1);
    distribution.update(5);
    distribution.update(6);
    distribution.update(7);

    Iterable<CounterUpdate> updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s1"))
                        .setMetadata(new CounterMetadata().setKind(Kind.DISTRIBUTION.toString())))
                .setCumulative(false)
                .setDistribution(
                    new DistributionUpdate()
                        .setCount(longToSplitInt(3))
                        .setMax(longToSplitInt(7))
                        .setMin(longToSplitInt(5))
                        .setSum(longToSplitInt(18)))));

    c1.getDistribution(name1).update(3);

    updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s1"))
                        .setMetadata(new CounterMetadata().setKind(Kind.DISTRIBUTION.toString())))
                .setCumulative(false)
                .setDistribution(
                    new DistributionUpdate()
                        .setCount(longToSplitInt(1))
                        .setMax(longToSplitInt(3))
                        .setMin(longToSplitInt(3))
                        .setSum(longToSplitInt(3)))));
  }

  @Test
  public void testGaugeUpdateExtraction() {

    // Freeze the clock, since gauge metrics depend on time.
    DateTimeUtils.setCurrentMillisFixed(10L);
    Gauge gauge = c1.getGauge(name1);
    gauge.set(7);
    gauge.set(5);
    gauge.set(6);

    // Only have the last update.
    Iterable<CounterUpdate> updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s1"))
                        .setMetadata(new CounterMetadata().setKind(Kind.LATEST_VALUE.toString())))
                .setCumulative(false)
                .setIntegerGauge(
                    new IntegerGauge()
                        .setValue(longToSplitInt(6))
                        .setTimestamp(org.joda.time.Instant.ofEpochMilli(10L).toString()))));

    DateTimeUtils.setCurrentMillisFixed(20L);
    gauge.set(8);

    assertThat(
        updates,
        containsInAnyOrder(
            new CounterUpdate()
                .setStructuredNameAndMetadata(
                    new CounterStructuredNameAndMetadata()
                        .setName(
                            new CounterStructuredName()
                                .setOrigin(Origin.USER.toString())
                                .setOriginNamespace("ns")
                                .setName("name1")
                                .setOriginalStepName("s1"))
                        .setMetadata(new CounterMetadata().setKind(Kind.LATEST_VALUE.toString())))
                .setCumulative(false)
                .setIntegerGauge(
                    new IntegerGauge()
                        .setValue(longToSplitInt(8))
                        .setTimestamp(org.joda.time.Instant.ofEpochMilli(20L).toString()))));

    // Release freeze on clock.
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testStringSetUpdateExtraction() {
    StringSet stringSet = c1.getStringSet(name1);
    stringSet.add("ab");
    stringSet.add("cd", "ef");
    stringSet.add("gh");
    stringSet.add("gh");

    CounterUpdate name1Update =
        new CounterUpdate()
            .setStructuredNameAndMetadata(
                new CounterStructuredNameAndMetadata()
                    .setName(
                        new CounterStructuredName()
                            .setOrigin(Origin.USER.toString())
                            .setOriginNamespace("ns")
                            .setName("name1")
                            .setOriginalStepName("s1"))
                    .setMetadata(new CounterMetadata().setKind(Kind.SET.toString())))
            .setCumulative(false)
            .setStringList(new StringList().setElements(Arrays.asList("ab", "cd", "ef", "gh")));

    Iterable<CounterUpdate> updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(updates, containsInAnyOrder(name1Update));

    stringSet = c2.getStringSet(name2);
    stringSet.add("ij");
    stringSet.add("kl", "mn");
    stringSet.add("mn");

    CounterUpdate name2Update =
        new CounterUpdate()
            .setStructuredNameAndMetadata(
                new CounterStructuredNameAndMetadata()
                    .setName(
                        new CounterStructuredName()
                            .setOrigin(Origin.USER.toString())
                            .setOriginNamespace("ns")
                            .setName("name2")
                            .setOriginalStepName("s2"))
                    .setMetadata(new CounterMetadata().setKind(Kind.SET.toString())))
            .setCumulative(false)
            .setStringList(new StringList().setElements(Arrays.asList("ij", "kl", "mn")));

    updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(updates, containsInAnyOrder(name1Update, name2Update));

    c1.getStringSet(name1).add("op");
    name1Update.setStringList(
        new StringList().setElements(Arrays.asList("ab", "cd", "ef", "gh", "op")));

    updates = StreamingStepMetricsContainer.extractMetricUpdates(registry);
    assertThat(updates, containsInAnyOrder(name1Update, name2Update));
  }

  @Test
  public void testPerWorkerMetrics() {
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(false);
    MetricsContainer metricsContainer = registry.getContainer("test_step");
    assertThat(
        metricsContainer.getPerWorkerCounter(name1), sameInstance(NoOpCounter.getInstance()));
    HistogramData.BucketType testBucket = HistogramData.LinearBuckets.of(1, 1, 1);
    assertThat(
        metricsContainer.getPerWorkerHistogram(name1, testBucket),
        sameInstance(NoOpHistogram.getInstance()));

    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    assertThat(metricsContainer.getPerWorkerCounter(name1), not(instanceOf(NoOpCounter.class)));
    assertThat(
        metricsContainer.getPerWorkerHistogram(name1, testBucket),
        not(instanceOf(NoOpHistogram.class)));
  }

  @Test
  public void testExtractPerWorkerMetricUpdates_populatedMetrics() {
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    MetricName counterMetricName = MetricName.named("BigQuerySink", "counter");
    c1.getPerWorkerCounter(counterMetricName).inc(3);

    MetricName histogramMetricName = MetricName.named("BigQuerySink", "histogram");
    HistogramData.LinearBuckets linearBuckets = HistogramData.LinearBuckets.of(0, 10, 10);
    c2.getPerWorkerHistogram(histogramMetricName, linearBuckets).update(5.0);

    Iterable<PerStepNamespaceMetrics> updates =
        StreamingStepMetricsContainer.extractPerWorkerMetricUpdates(registry);

    // Expected counter metric.
    MetricValue expectedCounter =
        new MetricValue().setMetric("counter").setMetricLabels(new HashMap<>()).setValueInt64(3L);

    PerStepNamespaceMetrics counters =
        new PerStepNamespaceMetrics()
            .setOriginalStep("s1")
            .setMetricsNamespace("BigQuerySink")
            .setMetricValues(Collections.singletonList(expectedCounter));

    // Expected histogram metric
    List<Long> bucketCounts = Collections.singletonList(1L);

    Linear linearOptions = new Linear().setNumberOfBuckets(10).setWidth(10.0).setStart(0.0);
    BucketOptions bucketOptions = new BucketOptions().setLinear(linearOptions);

    DataflowHistogramValue linearHistogram =
        new DataflowHistogramValue()
            .setCount(1L)
            .setBucketOptions(bucketOptions)
            .setBucketCounts(bucketCounts);

    MetricValue expectedHistogram =
        new MetricValue()
            .setMetric("histogram")
            .setMetricLabels(new HashMap<>())
            .setValueHistogram(linearHistogram);

    PerStepNamespaceMetrics histograms =
        new PerStepNamespaceMetrics()
            .setOriginalStep("s2")
            .setMetricsNamespace("BigQuerySink")
            .setMetricValues(Collections.singletonList(expectedHistogram));

    assertThat(updates, containsInAnyOrder(histograms, counters));
  }

  @Test
  public void testExtractPerWorkerMetricUpdates_emptyMetrics() {
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    StreamingStepMetricsContainer.setEnablePerWorkerMetrics(true);
    MetricName counterMetricName = MetricName.named("BigQuerySink", "counter");
    c1.getPerWorkerCounter(counterMetricName);

    MetricName histogramMetricName = MetricName.named("BigQuerySink", "histogram");
    HistogramData.LinearBuckets linearBuckets = HistogramData.LinearBuckets.of(0, 10, 10);
    c2.getPerWorkerHistogram(histogramMetricName, linearBuckets);

    Iterable<PerStepNamespaceMetrics> updates =
        StreamingStepMetricsContainer.extractPerWorkerMetricUpdates(registry);
    assertThat(updates, IsEmptyIterable.emptyIterable());
  }

  public class TestClock extends Clock {
    private Instant currentTime;

    public void advance(Duration amount) {
      currentTime = currentTime.plus(amount);
    }

    TestClock(Instant startTime) {
      currentTime = startTime;
    }

    @Override
    public Instant instant() {
      return currentTime;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    // Currently not supported.
    @Override
    public Clock withZone(ZoneId zone) {
      return new TestClock(currentTime);
    }
  }

  @Test
  public void testDeleteStaleCounters() {
    TestClock clock = new TestClock(Instant.now());
    Map<MetricName, Instant> countersByFirstStaleTime = new HashMap<>();
    ConcurrentHashMap<MetricName, AtomicLong> perWorkerCounters = new ConcurrentHashMap<>();
    ConcurrentHashMap<MetricName, LabeledMetricNameUtils.ParsedMetricName> parsedMetricNamesCache =
        new ConcurrentHashMap<>();

    StreamingStepMetricsContainer metricsContainer =
        StreamingStepMetricsContainer.forTesting(
            "s1", countersByFirstStaleTime, perWorkerCounters, parsedMetricNamesCache, clock);

    MetricName counterMetricName1 = MetricName.named("BigQuerySink", "counter1-");
    MetricName counterMetricName2 = MetricName.named("BigQuerySink", "counter2-");
    metricsContainer.getPerWorkerCounter(counterMetricName1).inc(3);
    metricsContainer.getPerWorkerCounter(counterMetricName2).inc(3);

    List<PerStepNamespaceMetrics> updatesList =
        Lists.newArrayList(metricsContainer.extractPerWorkerMetricUpdates());
    assertThat(updatesList.size(), equalTo(1));

    assertThat(perWorkerCounters.get(counterMetricName1).get(), equalTo(0L));
    assertThat(countersByFirstStaleTime.size(), equalTo(0));
    assertThat(parsedMetricNamesCache.size(), equalTo(2));

    // Verify that parsedMetricNames have been cached.
    LabeledMetricNameUtils.ParsedMetricName parsedCounter1 =
        LabeledMetricNameUtils.parseMetricName(counterMetricName1.getName()).get();
    LabeledMetricNameUtils.ParsedMetricName parsedCounter2 =
        LabeledMetricNameUtils.parseMetricName(counterMetricName2.getName()).get();

    assertThat(parsedMetricNamesCache.size(), equalTo(2));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName1, parsedCounter1));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName2, parsedCounter2));

    // At minute 1 both metrics are discovered to be zero-valued.
    updatesList = Lists.newArrayList(metricsContainer.extractPerWorkerMetricUpdates());
    assertThat(updatesList.size(), equalTo(0));

    assertThat(
        countersByFirstStaleTime.keySet(),
        containsInAnyOrder(counterMetricName1, counterMetricName2));
    assertThat(
        perWorkerCounters.keySet(), containsInAnyOrder(counterMetricName1, counterMetricName2));

    assertThat(parsedMetricNamesCache.size(), equalTo(2));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName1, parsedCounter1));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName2, parsedCounter2));

    // At minute 2 metric1 is zero-valued, metric2 has been updated.
    metricsContainer.getPerWorkerCounter(counterMetricName2).inc(3);
    clock.advance(Duration.ofSeconds(60));

    updatesList = Lists.newArrayList(metricsContainer.extractPerWorkerMetricUpdates());
    assertThat(updatesList.size(), equalTo(1));

    assertThat(countersByFirstStaleTime.keySet(), contains(counterMetricName1));
    assertThat(
        perWorkerCounters.keySet(), containsInAnyOrder(counterMetricName1, counterMetricName2));

    assertThat(parsedMetricNamesCache.size(), equalTo(2));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName1, parsedCounter1));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName2, parsedCounter2));

    // After minute 6 metric1 is still zero valued and should be cleaned up.
    metricsContainer.getPerWorkerCounter(counterMetricName2).inc(3);
    clock.advance(Duration.ofSeconds(4 * 60 + 1));

    updatesList = Lists.newArrayList(metricsContainer.extractPerWorkerMetricUpdates());
    assertThat(updatesList.size(), equalTo(1));

    assertThat(countersByFirstStaleTime.size(), equalTo(0));
    assertThat(perWorkerCounters.keySet(), contains(counterMetricName2));

    assertThat(parsedMetricNamesCache.size(), equalTo(1));
    assertThat(
        parsedMetricNamesCache, IsMapContaining.hasEntry(counterMetricName2, parsedCounter2));
  }
}
