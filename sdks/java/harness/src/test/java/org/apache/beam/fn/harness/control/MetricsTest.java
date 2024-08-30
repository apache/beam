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
package org.apache.beam.fn.harness.control;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.fn.harness.control.Metrics.BundleCounter;
import org.apache.beam.fn.harness.control.Metrics.BundleDistribution;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Metrics}. */
@RunWith(JUnit4.class)
public class MetricsTest {
  private static final MetricName TEST_NAME = MetricName.named("testNamespace", "testName");
  private static final String TEST_ID = "testId";

  private interface MetricMutator {
    void mutate();
  }

  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  @Test
  public void testAccurateBundleCounterReportsValueFirstTimeWithoutMutations() throws Exception {
    Map<String, ByteString> report = new HashMap<>();
    BundleCounter bundleCounter = Metrics.bundleProcessingThreadCounter(TEST_ID, TEST_NAME);
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(0)));
    report.clear();
    // Test that a reported value isn't reported again on final update
    bundleCounter.updateFinalMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    // Test that the value is not reported after reset if no mutations after being
    // reported the first time.
    bundleCounter.reset();
    bundleCounter.updateFinalMonitoringData(report);
    assertEquals(report, Collections.emptyMap());
  }

  @Test
  public void testAccurateBundleDistributionReportsValueFirstTimeWithoutMutations()
      throws Exception {
    Map<String, ByteString> report = new HashMap<>();
    BundleDistribution bundleDistribution =
        Metrics.bundleProcessingThreadDistribution(TEST_ID, TEST_NAME);
    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(
        report,
        Collections.singletonMap(
            TEST_ID, MonitoringInfoEncodings.encodeInt64Distribution(DistributionData.EMPTY)));
    report.clear();

    // Test that a reported value isn't reported again on final update
    bundleDistribution.updateFinalMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    // Test that the value is not reported after reset if no mutations after being
    // reported the first time.
    bundleDistribution.reset();
    bundleDistribution.updateFinalMonitoringData(report);
    assertEquals(report, Collections.emptyMap());
  }

  @Test
  public void testAccurateBundleCounterWithMutations() throws Exception {
    Map<String, ByteString> report = new HashMap<>();
    BundleCounter bundleCounter = Metrics.bundleProcessingThreadCounter(TEST_ID, TEST_NAME);

    bundleCounter.inc(7);
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(7)));
    report.clear();

    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    bundleCounter.inc();
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(8)));
    report.clear();

    bundleCounter.dec(4);
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(4)));

    bundleCounter.dec();
    bundleCounter.updateFinalMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(3)));

    // Test re-use of the metrics after reset
    bundleCounter.reset();
    bundleCounter.inc(7);
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(7)));
    report.clear();

    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    bundleCounter.inc();
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(8)));
    report.clear();

    bundleCounter.dec(4);
    bundleCounter.updateIntermediateMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(4)));

    bundleCounter.dec();
    bundleCounter.updateFinalMonitoringData(report);
    assertEquals(
        report, Collections.singletonMap(TEST_ID, MonitoringInfoEncodings.encodeInt64Counter(3)));
  }

  @Test
  public void testAccurateBundleDistributionWithMutations() throws Exception {
    Map<String, ByteString> report = new HashMap<>();
    BundleDistribution bundleDistribution =
        Metrics.bundleProcessingThreadDistribution(TEST_ID, TEST_NAME);

    bundleDistribution.update(7);
    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(
        report,
        Collections.singletonMap(
            TEST_ID,
            MonitoringInfoEncodings.encodeInt64Distribution(DistributionData.singleton(7))));
    report.clear();

    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    bundleDistribution.update(5, 2, 2, 3);
    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(
        report,
        Collections.singletonMap(
            TEST_ID,
            MonitoringInfoEncodings.encodeInt64Distribution(DistributionData.create(12, 3, 2, 7))));
    report.clear();

    // Test re-use of the metrics after reset
    bundleDistribution.reset();
    bundleDistribution.update(7);
    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(
        report,
        Collections.singletonMap(
            TEST_ID,
            MonitoringInfoEncodings.encodeInt64Distribution(DistributionData.singleton(7))));
    report.clear();

    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(report, Collections.emptyMap());

    bundleDistribution.update(5, 2, 2, 3);
    bundleDistribution.updateIntermediateMonitoringData(report);
    assertEquals(
        report,
        Collections.singletonMap(
            TEST_ID,
            MonitoringInfoEncodings.encodeInt64Distribution(DistributionData.create(12, 3, 2, 7))));
  }

  @Test
  public void testAccurateBundleCounterUsingMultipleThreads() throws Exception {
    BundleCounter bundleCounter = Metrics.bundleProcessingThreadCounter(TEST_ID, TEST_NAME);
    List<ByteString> values =
        testAccurateBundleMetricUsingMultipleThreads(bundleCounter, () -> bundleCounter.inc());
    assertTrue(values.size() >= 10);
    List<Long> sortedValues = new ArrayList<>();
    for (ByteString value : values) {
      sortedValues.add(MonitoringInfoEncodings.decodeInt64Counter(value));
    }
    Collections.sort(sortedValues);
    List<ByteString> sortedEncodedValues = new ArrayList<>();
    for (Long value : sortedValues) {
      sortedEncodedValues.add(MonitoringInfoEncodings.encodeInt64Counter(value));
    }
    assertThat(values, contains(sortedEncodedValues.toArray()));
  }

  @Test
  public void testAccurateBundleDistributionUsingMultipleThreads() throws Exception {
    BundleDistribution bundleDistribution =
        Metrics.bundleProcessingThreadDistribution(TEST_ID, TEST_NAME);
    List<ByteString> values =
        testAccurateBundleMetricUsingMultipleThreads(
            bundleDistribution, () -> bundleDistribution.update(1));

    assertTrue(values.size() >= 10);
    List<DistributionData> sortedValues = new ArrayList<>();
    for (ByteString value : values) {
      sortedValues.add(MonitoringInfoEncodings.decodeInt64Distribution(value));
    }
    Collections.sort(sortedValues, Comparator.comparingLong(DistributionData::count));
    List<ByteString> sortedEncodedValues = new ArrayList<>();
    for (DistributionData value : sortedValues) {
      sortedEncodedValues.add(MonitoringInfoEncodings.encodeInt64Distribution(value));
    }
    assertThat(values, contains(sortedEncodedValues.toArray()));
  }

  private List<ByteString> testAccurateBundleMetricUsingMultipleThreads(
      BundleProgressReporter metric, MetricMutator metricMutator) throws Exception {
    Lock progressLock = new ReentrantLock();
    List<ByteString> reportedValues = new ArrayList<>(); // Guarded by progressLock
    AtomicBoolean shouldMainExit = new AtomicBoolean();
    AtomicBoolean shouldProgressExit = new AtomicBoolean();
    Future<?> intermediateProgressTask =
        executor.submit(
            () -> {
              while (!shouldProgressExit.get()) {
                Map<String, ByteString> data = new HashMap<>();
                progressLock.lock();
                try {
                  metric.updateIntermediateMonitoringData(data);
                  if (!data.isEmpty()) {
                    reportedValues.add(data.get(TEST_ID));
                    if (reportedValues.size() >= 10) {
                      shouldMainExit.set(true);
                    }
                  }
                } finally {
                  progressLock.unlock();
                }
              }
              return null;
            });

    Future<?> mainTask =
        executor.submit(
            () -> {
              Map<String, ByteString> data = new HashMap<>();

              while (!shouldMainExit.get()) {
                metricMutator.mutate();
              }

              progressLock.lock();
              try {
                metric.updateFinalMonitoringData(data);
                if (!data.isEmpty()) {
                  reportedValues.add(data.get(TEST_ID));
                }
              } finally {
                progressLock.unlock();
              }
              shouldProgressExit.set(true);
              return null;
            });

    intermediateProgressTask.get();
    mainTask.get();

    return reportedValues;
  }
}
