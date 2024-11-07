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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricUpdateMatchers.metricUpdate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MetricsContainerImpl}. */
@RunWith(JUnit4.class)
public class MetricsContainerImplTest {

  @Test
  public void testCounterDeltas() {
    MetricsContainerImpl container = new MetricsContainerImpl("step1");
    CounterCell c1 = container.getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getCounter(MetricName.named("ns", "name2"));
    assertThat(
        "All counters should not start dirty",
        container.getUpdates().counterUpdates(),
        emptyIterable());

    c1.inc(5L);

    assertThat(
        "Dirty counter should be committed",
        container.getUpdates().counterUpdates(),
        containsInAnyOrder(metricUpdate("name1", 5L)));
    container.commitUpdates();
    assertThat(
        "After commit no counters should be dirty",
        container.getUpdates().counterUpdates(),
        emptyIterable());

    c1.inc(5L);
    c2.inc(4L);

    assertThat(
        container.getUpdates().counterUpdates(),
        containsInAnyOrder(metricUpdate("name1", 10L), metricUpdate("name2", 4L)));

    assertThat(
        "Since we haven't committed, updates are still included",
        container.getUpdates().counterUpdates(),
        containsInAnyOrder(metricUpdate("name1", 10L), metricUpdate("name2", 4L)));

    container.commitUpdates();
    assertThat(
        "After commit there are no updates",
        container.getUpdates().counterUpdates(),
        emptyIterable());

    c1.inc(8L);
    assertThat(container.getUpdates().counterUpdates(), contains(metricUpdate("name1", 18L)));

    CounterCell dne = container.tryGetCounter(MetricName.named("ns", "dne"));
    assertEquals(dne, null);
  }

  @Test
  public void testCounterCumulatives() {
    MetricsContainerImpl container = new MetricsContainerImpl("step1");
    CounterCell c1 = container.getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getCounter(MetricName.named("ns", "name2"));
    c1.inc(2L);
    c2.inc(4L);
    c1.inc(3L);

    container.getUpdates();
    container.commitUpdates();
    assertThat(
        "Committing updates shouldn't affect cumulative counter values",
        container.getCumulative().counterUpdates(),
        containsInAnyOrder(metricUpdate("name1", 5L), metricUpdate("name2", 4L)));

    c1.inc(8L);
    assertThat(
        container.getCumulative().counterUpdates(),
        containsInAnyOrder(metricUpdate("name1", 13L), metricUpdate("name2", 4L)));

    CounterCell readC1 = container.tryGetCounter(MetricName.named("ns", "name1"));
    assertEquals(13L, (long) readC1.getCumulative());
  }

  @Test
  public void testDistributionDeltas() {
    MetricsContainerImpl container = new MetricsContainerImpl("step1");
    DistributionCell c1 = container.getDistribution(MetricName.named("ns", "name1"));
    DistributionCell c2 = container.getDistribution(MetricName.named("ns", "name2"));

    assertThat(
        "Distributions don't start dirty",
        container.getUpdates().distributionUpdates(),
        emptyIterable());

    c1.update(5L);

    assertThat(
        "Dirty counter is updated",
        container.getUpdates().distributionUpdates(),
        containsInAnyOrder(metricUpdate("name1", DistributionData.create(5, 1, 5, 5))));

    container.commitUpdates();
    assertThat(
        "No updates after commit", container.getUpdates().distributionUpdates(), emptyIterable());

    c1.update(6L);
    c2.update(4L);

    assertThat(
        container.getUpdates().distributionUpdates(),
        containsInAnyOrder(
            metricUpdate("name1", DistributionData.create(11, 2, 5, 6)),
            metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));
    assertThat(
        "Updates stay the same without commit",
        container.getUpdates().distributionUpdates(),
        containsInAnyOrder(
            metricUpdate("name1", DistributionData.create(11, 2, 5, 6)),
            metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));

    container.commitUpdates();
    assertThat(
        "No updatess after commit", container.getUpdates().distributionUpdates(), emptyIterable());

    c1.update(8L);
    c1.update(4L);
    assertThat(
        container.getUpdates().distributionUpdates(),
        contains(metricUpdate("name1", DistributionData.create(23, 4, 4, 8))));
    container.commitUpdates();

    DistributionCell dne = container.tryGetDistribution(MetricName.named("ns", "dne"));
    assertEquals(dne, null);
  }

  @Test
  public void testMonitoringInfosArePopulatedForUserCounters() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    CounterCell c1 = testObject.getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = testObject.getCounter(MetricName.named("ns", "name2"));
    c1.inc(2L);
    c2.inc(4L);
    c1.inc(3L);

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1
        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name1")
        .setInt64SumValue(5)
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    SimpleMonitoringInfoBuilder builder2 = new SimpleMonitoringInfoBuilder();
    builder2
        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name2")
        .setInt64SumValue(4)
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    ArrayList<MonitoringInfo> actualMonitoringInfos = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }

    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build(), builder2.build()));
  }

  @Test
  public void testMonitoringInfosArePopulatedForUserDistributions() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    DistributionCell c1 = testObject.getDistribution(MetricName.named("ns", "name1"));
    DistributionCell c2 = testObject.getDistribution(MetricName.named("ns", "name2"));
    c1.update(5L);
    c2.update(4L);

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1
        .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name1")
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1")
        .setInt64DistributionValue(DistributionData.create(5, 1, 5, 5));

    SimpleMonitoringInfoBuilder builder2 = new SimpleMonitoringInfoBuilder();
    builder2
        .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name2")
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1")
        .setInt64DistributionValue(DistributionData.create(4, 1, 4, 4));

    ArrayList<MonitoringInfo> actualMonitoringInfos = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }

    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build(), builder2.build()));
  }

  @Test
  public void testMonitoringInfosArePopulatedForUserGauges() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    GaugeCell gaugeCell1 = testObject.getGauge(MetricName.named("ns", "name1"));
    GaugeCell gaugeCell2 = testObject.getGauge(MetricName.named("ns", "name2"));
    GaugeData gaugeData1 = GaugeData.create(3L);
    GaugeData gaugeData2 = GaugeData.create(4L);
    gaugeCell1.update(gaugeData1);
    gaugeCell2.update(gaugeData2);

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1
        .setUrn(MonitoringInfoConstants.Urns.USER_LATEST_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name1")
        .setInt64LatestValue(gaugeData1)
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    SimpleMonitoringInfoBuilder builder2 = new SimpleMonitoringInfoBuilder();
    builder2
        .setUrn(MonitoringInfoConstants.Urns.USER_LATEST_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name2")
        .setInt64LatestValue(gaugeData2)
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    List<MonitoringInfo> actualMonitoringInfos = new ArrayList<>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }

    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build(), builder2.build()));
  }

  @Test
  public void testMonitoringInfosArePopulatedForUserStringSets() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    StringSetCell stringSetCellA = testObject.getStringSet(MetricName.named("ns", "nameA"));
    StringSetCell stringSetCellB = testObject.getStringSet(MetricName.named("ns", "nameB"));
    stringSetCellA.add("A");
    stringSetCellB.add("BBB");

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1
        .setUrn(MonitoringInfoConstants.Urns.USER_SET_STRING)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "nameA")
        .setStringSetValue(stringSetCellA.getCumulative())
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    SimpleMonitoringInfoBuilder builder2 = new SimpleMonitoringInfoBuilder();
    builder2
        .setUrn(MonitoringInfoConstants.Urns.USER_SET_STRING)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "nameB")
        .setStringSetValue(stringSetCellB.getCumulative())
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "step1");

    List<MonitoringInfo> actualMonitoringInfos = new ArrayList<>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }

    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build(), builder2.build()));
  }

  @Test
  public void testMonitoringInfosArePopulatedForSystemDistributions() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PCOLLECTION, "pcoll1");
    DistributionCell c1 =
        testObject.getDistribution(
            MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.SAMPLED_BYTE_SIZE, labels));
    c1.update(5L);

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1
        .setUrn(MonitoringInfoConstants.Urns.SAMPLED_BYTE_SIZE)
        .setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "pcoll1")
        .setInt64DistributionValue(DistributionData.create(5, 1, 5, 5));

    ArrayList<MonitoringInfo> actualMonitoringInfos = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }

    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build()));
  }

  @Test
  public void testMonitoringInfosArePopulatedForABeamCounter() {
    MetricsContainerImpl testObject = new MetricsContainerImpl("step1");
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(MonitoringInfoConstants.Labels.PCOLLECTION, "pcollection");
    MetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
    CounterCell c1 = testObject.getCounter(name);
    c1.inc(2L);

    SimpleMonitoringInfoBuilder builder1 = new SimpleMonitoringInfoBuilder();
    builder1.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    builder1.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "pcollection");
    builder1.setInt64SumValue(2);

    ArrayList<MonitoringInfo> actualMonitoringInfos = new ArrayList<MonitoringInfo>();
    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actualMonitoringInfos.add(mi);
    }
    assertThat(actualMonitoringInfos, containsInAnyOrder(builder1.build()));
  }

  @Test
  public void testProcessWideMetricContainerThrowsWhenReset() {
    MetricsContainerImpl testObject = MetricsContainerImpl.createProcessWideContainer();
    CounterCell c1 = testObject.getCounter(MetricName.named("ns", "name1"));
    c1.inc(2L);

    assertThrows(RuntimeException.class, () -> testObject.reset());
  }

  @Test
  public void testEquals() {
    MetricsContainerImpl metricsContainerImpl = new MetricsContainerImpl("stepName");
    MetricsContainerImpl equal = new MetricsContainerImpl("stepName");
    Assert.assertEquals(metricsContainerImpl, equal);
    Assert.assertEquals(metricsContainerImpl.hashCode(), equal.hashCode());
  }

  @Test
  public void testDeltaCounters() {
    MetricName cName = MetricName.named("namespace", "counter");
    MetricName gName = MetricName.named("namespace", "gauge");
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 2, 5);
    MetricName hName = MetricName.named("namespace", "histogram");
    MetricName stringSetName = MetricName.named("namespace", "stringset");
    MetricName pwhName = MetricName.named("namespace", "perWorkerHistogram");

    MetricsContainerImpl prevContainer = new MetricsContainerImpl(null);
    prevContainer.getCounter(cName).inc(2L);
    prevContainer.getGauge(gName).set(4L);
    prevContainer.getStringSet(stringSetName).add("ab");
    // Set buckets counts to: [1,1,1,0,0,0,1]
    prevContainer.getHistogram(hName, bucketType).update(-1);
    prevContainer.getHistogram(hName, bucketType).update(1);
    prevContainer.getHistogram(hName, bucketType).update(3);
    prevContainer.getHistogram(hName, bucketType).update(20);

     // Set PerWorkerBucketCounts to [0,1,1,0,0,0,0]
     prevContainer.getPerWorkerHistogram(pwhName, bucketType).update(1);
     prevContainer.getPerWorkerHistogram(pwhName, bucketType).update(3);

    MetricsContainerImpl nextContainer = new MetricsContainerImpl(null);
    nextContainer.getCounter(cName).inc(9L);
    nextContainer.getGauge(gName).set(8L);
    nextContainer.getStringSet(stringSetName).add("cd");
    nextContainer.getStringSet(stringSetName).add("ab");
    // Set buckets counts to: [2,4,5,0,0,0,3]
    nextContainer.getHistogram(hName, bucketType).update(-1);
    nextContainer.getHistogram(hName, bucketType).update(-1);
    for (int i = 0; i < 4; i++) {
      nextContainer.getHistogram(hName, bucketType).update(1);
    }
    for (int i = 0; i < 5; i++) {
      nextContainer.getHistogram(hName, bucketType).update(3);
    }
    nextContainer.getHistogram(hName, bucketType).update(20);
    nextContainer.getHistogram(hName, bucketType).update(20);
    nextContainer.getHistogram(hName, bucketType).update(20);

     // Set PerWorkerBucketCounts to [1,0,0,0,0,0,1]
     nextContainer.getPerWorkerHistogram(pwhName, bucketType).update(-1);
     nextContainer.getPerWorkerHistogram(pwhName, bucketType).update(20);

    MetricsContainerImpl deltaContainer =
        MetricsContainerImpl.deltaContainer(prevContainer, nextContainer);
    // Expect counter value: 7 = 9 - 2
    long cValue = deltaContainer.getCounter(cName).getCumulative();
    assertEquals(7L, cValue);

    // Expect gauge value: 8.
    GaugeData gValue = deltaContainer.getGauge(gName).getCumulative();
    assertEquals(8L, gValue.value());

    // Expect most recent value of string set which is all unique strings
    StringSetData stringSetData = deltaContainer.getStringSet(stringSetName).getCumulative();
    assertEquals(ImmutableSet.of("ab", "cd"), stringSetData.stringSet());

    // Expect bucket counts: [1,3,4,0,0,0,2]
    assertEquals(
        1, deltaContainer.getHistogram(hName, bucketType).getCumulative().getBottomBucketCount());
    long[] expectedBucketCounts = (new long[] {3, 4, 0, 0, 0});
    for (int i = 0; i < expectedBucketCounts.length; i++) {
      assertEquals(
          expectedBucketCounts[i],
          deltaContainer.getHistogram(hName, bucketType).getCumulative().getCount(i));
    }
    assertEquals(
        2, deltaContainer.getHistogram(hName, bucketType).getCumulative().getTopBucketCount());

    // Expect per worker bucket counts: [1,0,0,0,0,0,1]
    assertEquals(
      1,
      deltaContainer
          .getPerWorkerHistogram(pwhName, bucketType)
          .getCumulative()
          .getBottomBucketCount());
  assertEquals(
      1,
      deltaContainer
          .getPerWorkerHistogram(pwhName, bucketType)
          .getCumulative()
          .getTopBucketCount());
  }

  @Test
  public void testNotEquals() {
    MetricsContainerImpl metricsContainerImpl = new MetricsContainerImpl("stepName");

    Assert.assertNotEquals(metricsContainerImpl, new Object());

    MetricsContainerImpl differentStepName = new MetricsContainerImpl("DIFFERENT");
    Assert.assertNotEquals(metricsContainerImpl, differentStepName);
    Assert.assertNotEquals(metricsContainerImpl.hashCode(), differentStepName.hashCode());

    MetricsContainerImpl differentCounters = new MetricsContainerImpl("stepName");
    differentCounters.getCounter(MetricName.named("namespace", "name"));
    Assert.assertNotEquals(metricsContainerImpl, differentCounters);
    Assert.assertNotEquals(metricsContainerImpl.hashCode(), differentCounters.hashCode());

    MetricsContainerImpl differentDistributions = new MetricsContainerImpl("stepName");
    differentDistributions.getDistribution(MetricName.named("namespace", "name"));
    Assert.assertNotEquals(metricsContainerImpl, differentDistributions);
    Assert.assertNotEquals(metricsContainerImpl.hashCode(), differentDistributions.hashCode());

    MetricsContainerImpl differentGauges = new MetricsContainerImpl("stepName");
    differentGauges.getGauge(MetricName.named("namespace", "name"));
    Assert.assertNotEquals(metricsContainerImpl, differentGauges);
    Assert.assertNotEquals(metricsContainerImpl.hashCode(), differentGauges.hashCode());

    MetricsContainerImpl differentStringSets = new MetricsContainerImpl("stepName");
    differentStringSets.getStringSet(MetricName.named("namespace", "name"));
    Assert.assertNotEquals(metricsContainerImpl, differentStringSets);
    Assert.assertNotEquals(metricsContainerImpl.hashCode(), differentStringSets.hashCode());
  }

  @Test
  public void testMatchMetric() {
    String urn = MonitoringInfoConstants.Urns.API_REQUEST_COUNT;
    Map<String, String> labels = new HashMap<String, String>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "MyPtransform");
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "BigQuery");
    labels.put(MonitoringInfoConstants.Labels.METHOD, "BigQueryBatchWrite");
    labels.put(MonitoringInfoConstants.Labels.RESOURCE, "Resource");
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_PROJECT_ID, "MyProject");
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_DATASET, "MyDataset");
    labels.put(MonitoringInfoConstants.Labels.BIGQUERY_TABLE, "MyTable");

    // MonitoringInfoMetricName will copy labels. So its safe to reuse this reference.
    labels.put(MonitoringInfoConstants.Labels.STATUS, "ok");
    MonitoringInfoMetricName okName = MonitoringInfoMetricName.named(urn, labels);
    labels.put(MonitoringInfoConstants.Labels.STATUS, "not_found");
    MonitoringInfoMetricName notFoundName = MonitoringInfoMetricName.named(urn, labels);

    Set<String> allowedMetricUrns = new HashSet<String>();
    allowedMetricUrns.add(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
    assertTrue(MetricsContainerImpl.matchMetric(okName, allowedMetricUrns));
    assertTrue(MetricsContainerImpl.matchMetric(notFoundName, allowedMetricUrns));

    MetricName userMetricName = MetricName.named("namespace", "name");
    assertFalse(MetricsContainerImpl.matchMetric(userMetricName, allowedMetricUrns));

    MetricName elementCountName =
        MonitoringInfoMetricName.named(
            MonitoringInfoConstants.Urns.ELEMENT_COUNT,
            Collections.singletonMap("name", "counter"));
    assertFalse(MetricsContainerImpl.matchMetric(elementCountName, allowedMetricUrns));
  }
}
