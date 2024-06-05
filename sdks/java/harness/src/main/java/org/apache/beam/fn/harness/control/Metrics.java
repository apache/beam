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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

public class Metrics {

  /**
   * A {@link Counter} that is designed to report intermediate and final results and can be re-used
   * after being reset.
   */
  public interface BundleCounter extends BundleProgressReporter, Counter {}

  /**
   * A {@link Distribution} that is designed to report intermediate and final results and can be
   * re-used after being reset.
   */
  public interface BundleDistribution extends BundleProgressReporter, Distribution {}

  /**
   * Returns a counter that will report an accurate final value.
   *
   * <p>All invocations to {@link Counter} methods must be done by the main bundle processing
   * thread.
   *
   * <p>All invocations to {@link BundleProgressReporter} methods must be done while holding the
   * {@link BundleProcessor#getProgressRequestLock}.
   */
  public static BundleCounter bundleProcessingThreadCounter(String shortId, MetricName name) {
    return new BundleProcessingThreadCounter(shortId, name);
  }

  /**
   * Returns a {@link Distribution} that will report an accurate final value.
   *
   * <p>All invocations to {@link Distribution} methods must be done by the main bundle processing
   * thread.
   *
   * <p>All invocations to {@link BundleProgressReporter} methods must be done while holding the
   * {@link BundleProcessor#getProgressRequestLock}.
   */
  public static BundleDistribution bundleProcessingThreadDistribution(
      String shortId, MetricName name) {
    return new BundleProcessingThreadDistribution(shortId, name);
  }

  /**
   * A {@link Counter} that shares the data using {@link AtomicReference#lazySet} to reduce
   * synchronization overhead. This guarantees that intermediate progress reports will report a
   * value that has been recently updated while the final progress report will get the true final
   * value.
   */
  @NotThreadSafe
  private static class BundleProcessingThreadCounter implements BundleCounter {
    private final MetricName name;
    private final String shortId;
    /** Guarded by {@link BundleProcessor#getProgressRequestLock}. */
    private boolean hasReportedValue;
    /** Guarded by {@link BundleProcessor#getProgressRequestLock}. */
    private long lastReportedValue;

    private final AtomicLong lazyCount;
    private long count;

    public BundleProcessingThreadCounter(String shortId, MetricName name) {
      this.shortId = shortId;
      this.name = name;
      this.lazyCount = new AtomicLong();
    }

    @Override
    public void inc() {
      count += 1;
      lazyCount.lazySet(count);
    }

    @Override
    public void inc(long n) {
      count += n;
      lazyCount.lazySet(count);
    }

    @Override
    public void dec() {
      count -= 1;
      lazyCount.lazySet(count);
    }

    @Override
    public void dec(long n) {
      count -= n;
      lazyCount.lazySet(count);
    }

    @Override
    public MetricName getName() {
      return name;
    }

    @Override
    public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
      long valueToReport = lazyCount.get();
      if (hasReportedValue && valueToReport == lastReportedValue) {
        return;
      }
      monitoringData.put(shortId, MonitoringInfoEncodings.encodeInt64Counter(valueToReport));
      lastReportedValue = valueToReport;
      hasReportedValue = true;
    }

    @Override
    public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
      if (hasReportedValue && count == lastReportedValue) {
        return;
      }
      monitoringData.put(shortId, MonitoringInfoEncodings.encodeInt64Counter(count));
      lastReportedValue = count;
      hasReportedValue = true;
    }

    @Override
    public void reset() {
      if (hasReportedValue) {
        count = 0;
        lazyCount.set(count);
        lastReportedValue = 0;
      }
    }
  }

  /**
   * A {@link Distribution} that shares the data using {@link AtomicReference#lazySet} to reduce
   * synchronization overhead. This guarantees that intermediate progress reports will report a
   * value that has been recently updated while the final progress report will get the true final
   * value.
   */
  @NotThreadSafe
  private static class BundleProcessingThreadDistribution implements BundleDistribution {
    private final MetricName name;
    private final String shortId;
    @Nullable private DistributionData lastReportedValue;
    private AtomicReference<DistributionData> lazyData;
    // Consider using a strategy that doesn't create a new object on each update
    private DistributionData data;

    public BundleProcessingThreadDistribution(String shortId, MetricName name) {
      this.shortId = shortId;
      this.name = name;
      this.data = DistributionData.EMPTY;
      this.lazyData = new AtomicReference<>(data);
      this.lastReportedValue = null;
    }

    @Override
    public void update(long value) {
      data = data.combine(value);
      lazyData.lazySet(data);
    }

    @Override
    public void update(long sum, long count, long min, long max) {
      data = data.combine(sum, count, min, max);
      lazyData.lazySet(data);
    }

    @Override
    public MetricName getName() {
      return name;
    }

    @Override
    public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
      DistributionData valueToReport = lazyData.get();
      if (valueToReport.equals(lastReportedValue)) {
        return;
      }
      monitoringData.put(shortId, MonitoringInfoEncodings.encodeInt64Distribution(lazyData.get()));
      lastReportedValue = valueToReport;
    }

    @Override
    public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
      if (data.equals(lastReportedValue)) {
        return;
      }
      monitoringData.put(shortId, MonitoringInfoEncodings.encodeInt64Distribution(data));
      lastReportedValue = data;
    }

    @Override
    public void reset() {
      // Use faster equality check
      if (lastReportedValue != null && lastReportedValue != DistributionData.EMPTY) {
        data = DistributionData.EMPTY;
        lazyData.set(DistributionData.EMPTY);
        lastReportedValue = DistributionData.EMPTY;
      }
    }
  }
}
