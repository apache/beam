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
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchWriteWithSummary;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Quality of Service manager options for Firestore RPCs.
 *
 * <p>Every RPC which is sent to Cloud Firestore is subject to QoS considerations. Successful,
 * failed, attempted requests are all tracked and directly drive the determination of when to
 * attempt an RPC.
 *
 * <p>Configuration of options can be accomplished by passing an instances of {@link RpcQosOptions}
 * to the {@code withRpcQosOptions} method of each {@code Builder} available in {@link FirestoreV1}.
 *
 * <p>A new instance of {@link RpcQosOptions.Builder} can be created via {@link
 * RpcQosOptions#newBuilder()}. A default instance of {@link RpcQosOptions} can be created via
 * {@link RpcQosOptions#defaultOptions()}.
 *
 * <p>
 *
 * @see FirestoreV1
 * @see FirestoreV1.BatchGetDocuments.Builder#withRpcQosOptions(RpcQosOptions)
 * @see BatchWriteWithSummary.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.ListCollectionIds.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.ListDocuments.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.PartitionQuery.Builder#withRpcQosOptions(RpcQosOptions)
 * @see FirestoreV1.RunQuery.Builder#withRpcQosOptions(RpcQosOptions)
 * @see <a target="_blank" rel="noopener noreferrer"
 *     href="https://cloud.google.com/firestore/quotas#limits">Standard limits</a>
 * @see <a target="_blank" rel="noopener noreferrer"
 *     href="https://cloud.google.com/firestore/docs/best-practices#designing_for_scale">Designing
 *     for scale</a>
 */
@Immutable
@ThreadSafe
public final class RpcQosOptions implements Serializable, HasDisplayData {

  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration samplePeriod;
  private final Duration samplePeriodBucketSize;
  private final double overloadRatio;
  private final Duration throttleDuration;
  private final int batchInitialCount;
  private final int batchMaxCount;
  private final long batchMaxBytes;
  private final Duration batchTargetLatency;
  private final int hintMaxNumWorkers;
  private final boolean shouldReportDiagnosticMetrics;

  private RpcQosOptions(
      int maxAttempts,
      Duration initialBackoff,
      Duration samplePeriod,
      Duration samplePeriodBucketSize,
      double overloadRatio,
      Duration throttleDuration,
      int batchInitialCount,
      int batchMaxCount,
      long batchMaxBytes,
      Duration batchTargetLatency,
      int hintMaxNumWorkers,
      boolean shouldReportDiagnosticMetrics) {
    this.maxAttempts = maxAttempts;
    this.initialBackoff = initialBackoff;
    this.samplePeriod = samplePeriod;
    this.samplePeriodBucketSize = samplePeriodBucketSize;
    this.overloadRatio = overloadRatio;
    this.throttleDuration = throttleDuration;
    this.batchInitialCount = batchInitialCount;
    this.batchMaxCount = batchMaxCount;
    this.batchMaxBytes = batchMaxBytes;
    this.batchTargetLatency = batchTargetLatency;
    this.hintMaxNumWorkers = hintMaxNumWorkers;
    this.shouldReportDiagnosticMetrics = shouldReportDiagnosticMetrics;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .add(DisplayData.item("maxAttempts", maxAttempts).withLabel("maxAttempts"))
        .add(DisplayData.item("initialBackoff", initialBackoff).withLabel("initialBackoff"))
        .add(DisplayData.item("samplePeriod", samplePeriod).withLabel("samplePeriod"))
        .add(
            DisplayData.item("samplePeriodBucketSize", samplePeriodBucketSize)
                .withLabel("samplePeriodBucketSize"))
        .add(DisplayData.item("overloadRatio", overloadRatio).withLabel("overloadRatio"))
        .add(DisplayData.item("throttleDuration", throttleDuration).withLabel("throttleDuration"))
        .add(
            DisplayData.item("batchInitialCount", batchInitialCount).withLabel("batchInitialCount"))
        .add(DisplayData.item("batchMaxCount", batchMaxCount).withLabel("batchMaxCount"))
        .add(DisplayData.item("batchMaxBytes", batchMaxBytes).withLabel("batchMaxBytes"))
        .add(
            DisplayData.item("batchTargetLatency", batchTargetLatency)
                .withLabel("batchTargetLatency"))
        .add(
            DisplayData.item("hintMaxNumWorkers", hintMaxNumWorkers).withLabel("hintMaxNumWorkers"))
        .add(
            DisplayData.item("shouldReportDiagnosticMetrics", shouldReportDiagnosticMetrics)
                .withLabel("shouldReportDiagnosticMetrics"));
  }

  /**
   * The maximum number of times a request will be attempted for a complete successful result.
   *
   * <p>For a stream based response, the full stream read must complete within the specified number
   * of attempts. Restarting a stream will count as a new attempt.
   *
   * <p><i>Default Value:</i> 5
   *
   * @see RpcQosOptions.Builder#withMaxAttempts(int)
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * The initial backoff duration to be used before retrying a request for the first time.
   *
   * <p><i>Default Value:</i> 5 sec
   *
   * @see RpcQosOptions.Builder#withInitialBackoff(Duration)
   */
  public Duration getInitialBackoff() {
    return initialBackoff;
  }

  /**
   * The length of time sampled request data will be retained.
   *
   * <p><i>Default Value:</i> 2 min
   *
   * @see RpcQosOptions.Builder#withSamplePeriod(Duration)
   */
  public Duration getSamplePeriod() {
    return samplePeriod;
  }

  /**
   * The size of buckets within the specified {@link #getSamplePeriod() samplePeriod}.
   *
   * <p><i>Default Value:</i> 10 sec
   *
   * @see RpcQosOptions.Builder#withSamplePeriodBucketSize(Duration)
   */
  public Duration getSamplePeriodBucketSize() {
    return samplePeriodBucketSize;
  }

  /**
   * The target ratio between requests sent and successful requests. This is "K" in the formula in
   * <a target="_blank" rel="noopener noreferrer"
   * href="https://landing.google.com/sre/book/chapters/handling-overload.html">SRE Book - Handling
   * Overload</a>
   *
   * <p><i>Default Value:</i> 1.05
   *
   * @see RpcQosOptions.Builder#withOverloadRatio(double)
   */
  public double getOverloadRatio() {
    return overloadRatio;
  }

  /**
   * The amount of time an attempt will be throttled if deemed necessary based on previous success
   * rate.
   *
   * <p><i>Default value:</i> 5 sec
   *
   * @see RpcQosOptions.Builder#withThrottleDuration(Duration)
   */
  public Duration getThrottleDuration() {
    return throttleDuration;
  }

  /**
   * The initial size of a batch; used in the absence of the QoS system having significant data to
   * determine a better batch size.
   *
   * <p><i>Default Value:</i> 20
   *
   * @see RpcQosOptions.Builder#withBatchInitialCount(int)
   */
  public int getBatchInitialCount() {
    return batchInitialCount;
  }

  /**
   * The maximum number of writes to include in a batch. (Used in the absence of the QoS system
   * having significant data to determine a better value). The actual number of writes per request
   * may be lower if we reach {@link #getBatchMaxBytes() batchMaxBytes}.
   *
   * <p><i>Default Value:</i> 500
   *
   * @see RpcQosOptions.Builder#withBatchMaxCount(int)
   */
  public int getBatchMaxCount() {
    return batchMaxCount;
  }

  /**
   * The maximum number of bytes to include in a batch. (Used in the absence of the QoS system
   * having significant data to determine a better value). The actual number of bytes per request
   * may be lower if {@link #getBatchMaxCount() batchMaxCount} is reached first.
   *
   * <p><i>Default Value:</i> 9.5 MiB
   *
   * @see RpcQosOptions.Builder#withBatchMaxBytes(long)
   */
  public long getBatchMaxBytes() {
    return batchMaxBytes;
  }

  /**
   * Target latency for batch requests. It aims for a target response time per RPC: the response
   * times for previous RPCs and the number of writes contained in them, calculates a rolling
   * average time-per-write, and chooses the number of writes for future requests to hit the target
   * time.
   *
   * <p><i>Default Value:</i> 5 sec
   *
   * @see RpcQosOptions.Builder#withBatchTargetLatency(Duration)
   */
  public Duration getBatchTargetLatency() {
    return batchTargetLatency;
  }

  /**
   * A hint to the QoS system for the intended max number of workers for a pipeline. The provided
   * value can be used to try to scale calculations to values appropriate for the pipeline as a
   * whole.
   *
   * <p>If you are running your pipeline on <a target="_blank" rel="noopener noreferrer"
   * href="https://cloud.google.com/dataflow">Cloud Dataflow</a> this parameter should be set to the
   * same value as <a target="_blank" rel="noopener noreferrer"
   * href="https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options">{@code
   * maxNumWorkers}</a>
   *
   * <p><i>Default Value:</i> 500
   *
   * @see RpcQosOptions.Builder#withBatchMaxCount(int)
   */
  public int getHintMaxNumWorkers() {
    return hintMaxNumWorkers;
  }

  /**
   * Whether additional diagnostic metrics should be reported for a Transform.
   *
   * <p>If true, additional detailed diagnostic metrics will be reported for the RPC QoS subsystem.
   *
   * <p>This parameter should be used with care as it will output ~40 additional custom metrics
   * which will count toward any possible pipeline metrics limits (For example <a target="_blank"
   * rel="noopener noreferrer"
   * href="https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring#custom_metrics">Dataflow
   * Custom Metrics</a> limits and <a target="_blank" rel="noopener noreferrer"
   * href="https://cloud.google.com/monitoring/quotas#custom_metrics_quotas">Cloud Monitoring custom
   * metrics quota</a>)
   *
   * <p><i>Default Value:</i> {@code false}
   *
   * @see RpcQosOptions.Builder#withReportDiagnosticMetrics()
   */
  public boolean isShouldReportDiagnosticMetrics() {
    return shouldReportDiagnosticMetrics;
  }

  /**
   * Create a new {@link Builder} initialized with the values from this instance.
   *
   * @return a new {@link Builder} initialized with the values from this instance.
   */
  public Builder toBuilder() {
    Builder builder =
        new Builder()
            .withMaxAttempts(maxAttempts)
            .withInitialBackoff(initialBackoff)
            .withSamplePeriod(samplePeriod)
            .withSamplePeriodBucketSize(samplePeriodBucketSize)
            .withOverloadRatio(overloadRatio)
            .withThrottleDuration(throttleDuration)
            .withBatchInitialCount(batchInitialCount)
            .withBatchMaxCount(batchMaxCount)
            .withBatchMaxBytes(batchMaxBytes)
            .withBatchTargetLatency(batchTargetLatency)
            .withHintMaxNumWorkers(hintMaxNumWorkers);
    if (shouldReportDiagnosticMetrics) {
      return builder.withReportDiagnosticMetrics();
    } else {
      return builder;
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RpcQosOptions)) {
      return false;
    }
    RpcQosOptions that = (RpcQosOptions) o;
    return maxAttempts == that.maxAttempts
        && Double.compare(that.overloadRatio, overloadRatio) == 0
        && batchInitialCount == that.batchInitialCount
        && batchMaxCount == that.batchMaxCount
        && batchMaxBytes == that.batchMaxBytes
        && hintMaxNumWorkers == that.hintMaxNumWorkers
        && shouldReportDiagnosticMetrics == that.shouldReportDiagnosticMetrics
        && initialBackoff.equals(that.initialBackoff)
        && samplePeriod.equals(that.samplePeriod)
        && samplePeriodBucketSize.equals(that.samplePeriodBucketSize)
        && throttleDuration.equals(that.throttleDuration)
        && batchTargetLatency.equals(that.batchTargetLatency);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        maxAttempts,
        initialBackoff,
        samplePeriod,
        samplePeriodBucketSize,
        overloadRatio,
        throttleDuration,
        batchInitialCount,
        batchMaxCount,
        batchMaxBytes,
        batchTargetLatency,
        hintMaxNumWorkers,
        shouldReportDiagnosticMetrics);
  }

  @Override
  public String toString() {
    return "RpcQosOptions{"
        + "maxAttempts="
        + maxAttempts
        + ", initialBackoff="
        + initialBackoff
        + ", samplePeriod="
        + samplePeriod
        + ", samplePeriodBucketSize="
        + samplePeriodBucketSize
        + ", overloadRatio="
        + overloadRatio
        + ", throttleDuration="
        + throttleDuration
        + ", batchInitialCount="
        + batchInitialCount
        + ", batchMaxCount="
        + batchMaxCount
        + ", batchMaxBytes="
        + batchMaxBytes
        + ", batchTargetLatency="
        + batchTargetLatency
        + ", hintMaxNumWorkers="
        + hintMaxNumWorkers
        + ", shouldReportDiagnosticMetrics="
        + shouldReportDiagnosticMetrics
        + '}';
  }

  /**
   * Factory method to return a new instance of {@link RpcQosOptions} with all default values.
   *
   * @return New instance of {@link RpcQosOptions} with all default values
   * @see #newBuilder()
   */
  public static RpcQosOptions defaultOptions() {
    return newBuilder().build();
  }

  /**
   * Factory method to return a new instance of {@link Builder} with all values set to their initial
   * default values.
   *
   * @return New instance of {@link Builder} with all values set to their initial default values
   * @see #defaultOptions()
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Mutable Builder class for creating instances of {@link RpcQosOptions}.
   *
   * <p>A new instance of {@link RpcQosOptions.Builder} can be created via {@link
   * RpcQosOptions#newBuilder()}.
   *
   * <p><i>NOTE:</i> All {@code with} methods in this class function as set rather than copy with
   * new value
   */
  public static final class Builder {

    /**
     * Cloud Firestore has a limit of 10MB per RPC. This is set lower than the 10MB limit on the
     * RPC, as this only accounts for the mutations themselves and not the Request wrapper around
     * them.
     */
    private static final long FIRESTORE_RPC_BYTES_MAX = (long) (9.5 * 1024 * 1024);
    /** The Cloud Firestore API has a limit of 500 document updates per request. */
    private static final int FIRESTORE_SINGLE_REQUEST_UPDATE_DOCUMENTS_MAX = 500;

    private int maxAttempts;
    private Duration initialBackoff;
    private Duration samplePeriod;
    private Duration samplePeriodBucketSize;
    private double overloadRatio;
    private Duration throttleDuration;
    private int batchInitialCount;
    private int batchMaxCount;
    private long batchMaxBytes;
    private Duration batchTargetLatency;
    private int hintMaxNumWorkers;
    private boolean shouldReportDiagnosticMetrics;

    private Builder() {
      maxAttempts = 5;
      initialBackoff = Duration.standardSeconds(5);
      samplePeriod = Duration.standardMinutes(2);
      samplePeriodBucketSize = Duration.standardSeconds(10);
      overloadRatio = 1.05;
      throttleDuration = Duration.standardSeconds(5);
      batchInitialCount = 20;
      batchMaxCount = FIRESTORE_SINGLE_REQUEST_UPDATE_DOCUMENTS_MAX;
      batchMaxBytes = FIRESTORE_RPC_BYTES_MAX;
      batchTargetLatency = Duration.standardSeconds(5);
      hintMaxNumWorkers = 500;
      shouldReportDiagnosticMetrics = false;
    }

    /**
     * Configure the maximum number of times a request will be attempted for a complete successful
     * result.
     *
     * <p>For a stream based response, the full stream read must complete within the specified
     * number of attempts. Restarting a stream will count as a new attempt.
     *
     * <p><i>Default Value:</i> 5
     *
     * @param maxAttempts an int in the range 1 <= {@code maxAttempts} <= 5
     * @return this builder
     * @see RpcQosOptions#getMaxAttempts()
     */
    public Builder withMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    /**
     * Configure the initial backoff duration to be used before retrying a request for the first
     * time.
     *
     * <p><i>Default Value:</i> 5 sec
     *
     * @param initialBackoff a {@link Duration} in the range 5 sec <= {@code initialBackoff} <= 2
     *     min
     * @return this builder
     * @see RpcQosOptions#getInitialBackoff()
     */
    public Builder withInitialBackoff(Duration initialBackoff) {
      this.initialBackoff = initialBackoff;
      return this;
    }

    /**
     * Configure the length of time sampled request data will be retained.
     *
     * <p><i>Default Value:</i> 2 min
     *
     * @param samplePeriod a {@link Duration} in the range 2 min <= {@code samplePeriod} <= 20 min
     * @return this builder
     * @see RpcQosOptions#getSamplePeriod()
     */
    public Builder withSamplePeriod(Duration samplePeriod) {
      this.samplePeriod = samplePeriod;
      return this;
    }

    /**
     * Configure the size of buckets within the specified {@link #withSamplePeriod(Duration)
     * samplePeriod}.
     *
     * <p><i>Default Value:</i> 10 sec
     *
     * @param samplePeriodBucketSize a {@link Duration} in the range 10 sec <= {@code
     *     samplePeriodBucketSize} <= 20 min
     * @return this builder
     * @see RpcQosOptions#getSamplePeriodBucketSize()
     */
    public Builder withSamplePeriodBucketSize(Duration samplePeriodBucketSize) {
      this.samplePeriodBucketSize = samplePeriodBucketSize;
      return this;
    }

    /**
     * The target ratio between requests sent and successful requests. This is "K" in the formula in
     * <a target="_blank" rel="noopener noreferrer"
     * href="https://sre.google/sre-book/handling-overload/#e2101">SRE Book - Handling Overload</a>
     *
     * <p><i>Default Value:</i> 1.05
     *
     * @param overloadRatio the target ratio between requests sent and successful requests. A double
     *     in the range 1.0 <= {@code overloadRatio} <= 1.5
     * @return this builder
     * @see RpcQosOptions#getOverloadRatio()
     */
    public Builder withOverloadRatio(double overloadRatio) {
      this.overloadRatio = overloadRatio;
      return this;
    }

    /**
     * Configure the amount of time an attempt will be throttled if deemed necessary based on
     * previous success rate.
     *
     * <p><i>Default value:</i> 5 sec
     *
     * @param throttleDuration a {@link Duration} in the range 5 sec <= {@code throttleDuration} <=
     *     1 min
     * @return this builder
     * @see RpcQosOptions#getThrottleDuration()
     */
    public Builder withThrottleDuration(Duration throttleDuration) {
      this.throttleDuration = throttleDuration;
      return this;
    }

    /**
     * Configure the initial size of a batch; used in the absence of the QoS system having
     * significant data to determine a better batch size.
     *
     * <p><i>Default Value:</i> 20
     *
     * @param batchInitialCount an int in the range 1 <= {@code batchInitialCount} <= 500
     * @return this builder
     * @see RpcQosOptions#getBatchInitialCount()
     */
    public Builder withBatchInitialCount(int batchInitialCount) {
      this.batchInitialCount = batchInitialCount;
      return this;
    }

    /**
     * Configure the maximum number of writes to include in a batch. (Used in the absence of the QoS
     * system having significant data to determine a better value). The actual number of writes per
     * request may be lower if {@link #withBatchMaxBytes(long) batchMaxBytes} is reached first.
     *
     * <p><i>Default Value:</i> 500
     *
     * @param batchMaxCount an int in the range 1 <= {@code batchMaxCount} <= 500
     * @return this builder
     * @see RpcQosOptions#getBatchMaxCount()
     */
    public Builder withBatchMaxCount(int batchMaxCount) {
      this.batchMaxCount = batchMaxCount;
      return this;
    }

    /**
     * Configure the maximum number of bytes to include in a batch. (Used in the absence of the QoS
     * system having significant data to determine a better value). The actual number of bytes per
     * request may be lower if {@link #withBatchMaxCount(int) batchMaxCount} is reached first.
     *
     * <p><i>Default Value:</i> 9.5 MiB
     *
     * @param batchMaxBytes an int in the range 1 B <= {@code batchMaxBytes} <= 9.5 MiB
     * @return this builder
     * @see RpcQosOptions#getBatchMaxBytes()
     */
    public Builder withBatchMaxBytes(long batchMaxBytes) {
      this.batchMaxBytes = batchMaxBytes;
      return this;
    }

    /**
     * Target latency for batch requests. It aims for a target response time per RPC: the response
     * times for previous RPCs and the number of writes contained in them, calculates a rolling
     * average time-per-write, and chooses the number of writes for future requests to hit the
     * target time.
     *
     * <p><i>Default Value:</i> 5 sec
     *
     * @param batchTargetLatency a {@link Duration} in the range 5 sec <= {@code batchTargetLatency}
     *     <= 2 min
     * @return this builder
     * @see RpcQosOptions#getBatchTargetLatency()
     */
    public Builder withBatchTargetLatency(Duration batchTargetLatency) {
      this.batchTargetLatency = batchTargetLatency;
      return this;
    }

    /**
     * Provide a hint to the QoS system for the intended max number of workers for a pipeline. The
     * provided value can be used to try to scale calculations to values appropriate for the
     * pipeline as a whole.
     *
     * <p>If you are running your pipeline on <a target="_blank" rel="noopener noreferrer"
     * href="https://cloud.google.com/dataflow">Cloud Dataflow</a> this parameter should be set to
     * the same value as <a target="_blank" rel="noopener noreferrer"
     * href="https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options">{@code
     * maxNumWorkers}</a>
     *
     * <p><i>Default Value:</i> 500
     *
     * @param hintMaxNumWorkers an int in the range 1 <= {@code hintMaxNumWorkers} <=
     *     Integer.MAX_VALUE
     * @return this builder
     * @see RpcQosOptions#getHintMaxNumWorkers()
     */
    public Builder withHintMaxNumWorkers(int hintMaxNumWorkers) {
      this.hintMaxNumWorkers = hintMaxNumWorkers;
      return this;
    }

    /**
     * Whether additional diagnostic metrics should be reported for a Transform.
     *
     * <p>If invoked on this builder, additional detailed diagnostic metrics will be reported for
     * the RPC QoS subsystem.
     *
     * <p>This parameter should be used with care as it will output ~40 additional custom metrics
     * which will count toward any possible pipeline metrics limits (For example <a target="_blank"
     * rel="noopener noreferrer"
     * href="https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring#custom_metrics">Dataflow
     * Custom Metrics</a> limits and <a target="_blank" rel="noopener noreferrer"
     * href="https://cloud.google.com/monitoring/quotas#custom_metrics_quotas">Cloud Monitoring
     * custom metrics quota</a>)
     *
     * @return this builder
     * @see RpcQosOptions#isShouldReportDiagnosticMetrics()
     */
    public Builder withReportDiagnosticMetrics() {
      this.shouldReportDiagnosticMetrics = true;
      return this;
    }

    /**
     * Create a new instance of {@link RpcQosOptions} from the current builder state.
     *
     * <p>All provided values will be validated for non-nullness and that each value falls within
     * its allowed range.
     *
     * @return a new instance of {@link RpcQosOptions} from the current builder state.
     * @throws NullPointerException if any nullable value is null
     * @throws IllegalArgumentException if any provided value does not fall within its allowed range
     */
    public RpcQosOptions build() {
      validateIndividualFields();
      validateRelatedFields();
      return unsafeBuild();
    }

    @VisibleForTesting
    RpcQosOptions unsafeBuild() {
      return new RpcQosOptions(
          maxAttempts,
          initialBackoff,
          samplePeriod,
          samplePeriodBucketSize,
          overloadRatio,
          throttleDuration,
          batchInitialCount,
          batchMaxCount,
          batchMaxBytes,
          batchTargetLatency,
          hintMaxNumWorkers,
          shouldReportDiagnosticMetrics);
    }

    @VisibleForTesting
    void validateIndividualFields() {
      checkInRange("maxAttempts", 1, 5, maxAttempts, Integer::compare);
      requireNonNull(initialBackoff, "initialBackoff must be non null");
      checkInRange(
          "initialBackoff",
          Duration.standardSeconds(5),
          Duration.standardMinutes(2),
          initialBackoff,
          Duration::compareTo);
      requireNonNull(samplePeriod, "samplePeriod must be non null");
      checkInRange(
          "samplePeriod",
          Duration.standardMinutes(2),
          Duration.standardMinutes(20),
          samplePeriod,
          Duration::compareTo);
      requireNonNull(samplePeriodBucketSize, "samplePeriodBucketSize must be non null");
      checkInRange(
          "samplePeriodBucketSize",
          Duration.standardSeconds(10),
          Duration.standardMinutes(20),
          samplePeriodBucketSize,
          Duration::compareTo);
      checkInRange("overloadRatio", 1.0, 1.5, overloadRatio, Double::compare);
      requireNonNull(throttleDuration, "throttleDuration must be non null");
      checkInRange(
          "throttleDuration",
          Duration.standardSeconds(5),
          Duration.standardMinutes(1),
          throttleDuration,
          Duration::compareTo);
      checkInRange(
          "batchInitialCount",
          1,
          FIRESTORE_SINGLE_REQUEST_UPDATE_DOCUMENTS_MAX,
          batchInitialCount,
          Integer::compare);
      checkInRange(
          "batchMaxCount",
          1,
          FIRESTORE_SINGLE_REQUEST_UPDATE_DOCUMENTS_MAX,
          batchMaxCount,
          Integer::compare);
      checkInRange("batchMaxBytes", 1L, FIRESTORE_RPC_BYTES_MAX, batchMaxBytes, Long::compare);
      requireNonNull(batchTargetLatency, "batchTargetLatency must be non null");
      checkInRange(
          "batchTargetLatency",
          Duration.standardSeconds(5),
          Duration.standardMinutes(2),
          batchTargetLatency,
          Duration::compareTo);
      checkInRange("hintWorkerCount", 1, Integer.MAX_VALUE, hintMaxNumWorkers, Integer::compare);
    }

    @VisibleForTesting
    void validateRelatedFields() {
      checkArgument(
          samplePeriodBucketSize.compareTo(samplePeriod) <= 0,
          String.format(
              "expected samplePeriodBucketSize <= samplePeriod, but was %s <= %s",
              samplePeriodBucketSize, samplePeriod));
    }

    private static <T> void checkInRange(
        String fieldName, T min, T max, T actual, Comparator<T> comp) {
      checkArgument(
          0 <= comp.compare(actual, min) && comp.compare(actual, max) <= 0,
          String.format(
              "%s must be in the range %s to %s, but was %s", fieldName, min, max, actual));
    }
  }
}
