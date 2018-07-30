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

package org.apache.beam.sdk.io.synthetic;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * This {@link SyntheticStep} class provides a parameterizable {@code DoFn} that consumes {@code
 * KV<byte[], byte[]>} and emits {@code KV<byte[], byte[]>}. Each emitted record will be slowed down
 * by a certain sleep time generated based on the specified sleep time distribution in {@link
 * SyntheticStep.Options}.
 *
 * <p>See {@link SyntheticStep.Options} for how to construct an instance. To construct a {@code
 * SyntheticStep} from SyntheticStep options, use:
 *
 * <pre>{@code
 * SyntheticStep.Options options = ...;
 *
 * // Construct the synthetic step with options.
 * SyntheticStep step = new SyntheticStep(options);
 * }</pre>
 */
public class SyntheticStep extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {
  private final Options options;
  private final KV<Long, Long> idAndThroughput; // used when maxWorkerThroughput is set
  private final Counter throttlingMsecsCounter =
      Metrics.counter("dataflow-throttling-metrics", "throttling-msecs");

  public SyntheticStep(Options options) {
    options.validate();
    this.options = options;
    Random rand = new Random();
    // use a random id so that a pipeline could have multiple SyntheticSteps
    this.idAndThroughput = KV.of(rand.nextLong(), options.maxWorkerThroughput);
  }

  private KV<byte[], byte[]> outputElement(
      byte[] inputKey, byte[] inputVal, long hashCodeOfVal, int index, Random rnd) {
    long seed = options.hashFunction().hashLong(hashCodeOfVal + index).asLong();
    Duration delayMsec = Duration.millis(options.nextDelay(seed));
    long sleepMillis = 0;
    while (delayMsec.getMillis() > 0) {
      sleepMillis +=
          SyntheticUtils.delay(
              delayMsec, options.cpuUtilizationInMixedDelay, options.delayType, rnd);

      // Ensure the worker throughput is within the limit (if set).
      if (options.maxWorkerThroughput < 0
          || rateLimiterCache.getUnchecked(idAndThroughput).tryAcquire()) {
        break;
      } else { // try 1 millis extra delay
        delayMsec = Duration.millis(1);
      }
    }

    if (options.reportThrottlingMicros && sleepMillis > 0) {
      throttlingMsecsCounter.inc(TimeUnit.MILLISECONDS.toMicros(sleepMillis));
    }

    if (options.preservesInputKeyDistribution) {
      // Generate the new byte array value whose hashcode will be
      // used as seed to initialize a Random object in next stages.
      byte[] newValue = new byte[inputVal.length];
      rnd.nextBytes(newValue);
      return KV.of(inputKey, newValue);
    } else {
      return options.genKvPair(seed);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    byte[] key = c.element().getKey();
    byte[] val = c.element().getValue();
    int decimalPart = (int) options.outputRecordsPerInputRecord;
    double fractionalPart = options.outputRecordsPerInputRecord - decimalPart;
    // Use the hashcode of val as seed to make the test deterministic.
    long hashCodeOfVal = options.hashFunction().hashBytes(val).asLong();
    Random random = new Random(hashCodeOfVal);
    int i;
    for (i = 0; i < decimalPart; i++) {
      c.output(outputElement(key, val, hashCodeOfVal, i, random));
    }
    if (random.nextDouble() < fractionalPart) {
      c.output(outputElement(key, val, hashCodeOfVal, i, random));
    }
  }

  @StartBundle
  public void startBundle() throws Exception {
    if (options.perBundleDelay > 0) {
      SyntheticUtils.delay(
          Duration.millis(options.perBundleDelay),
          options.cpuUtilizationInMixedDelay,
          options.perBundleDelayType,
          new Random());
    }
  }

  /**
   * Synthetic step options. These options are all JSON, see documentations of individual fields for
   * details. {@code Options} uses jackson annotations which PipelineOptionsFactory can use to parse
   * and construct an instance.
   */
  public static class Options extends SyntheticOptions {
    /**
     * Amplification/filtering ratio: the number of output records should be emitted on average for
     * each input record.
     */
    @JsonProperty public double outputRecordsPerInputRecord;

    /**
     * If false, the DoFn generates a different distribution of KV pairs according to the parameters
     * in {@link SyntheticOptions}, and input records are merely used as a “clock”; If true, the
     * shape of the input distribution is preserved, and the DoFn only does sleeping and
     * amplification/filtering.
     */
    @JsonProperty public boolean preservesInputKeyDistribution;

    /** User-defined tag to annotate synthetic step name. */
    @JsonProperty public String uniqueTag;

    /**
     * An upper limit on throughput across the worker for this step. In a streaming job, it is not
     * easy to tightly control parallelism of a DoFn. It depends on various factors. As a result, it
     * is much harder to control throughput preconfigured cpu or sleep delay. When max throughput is
     * set, SyntheticStep delays beyond the configured delay (either cpu or sleep) in order to keep
     * the overall throughput below the limit.
     */
    @JsonProperty public long maxWorkerThroughput = -1;

    /**
     * Number of milliseconds to delay for in each bundle. Cannot be enabled simultaneously with
     * maxWorkerThroughput >= 0.
     */
    @JsonProperty public long perBundleDelay = 0;

    /** Type of per bundle delay to use ("SLEEP", "CPU", "MIXED"). */
    @JsonProperty public DelayType perBundleDelayType = DelayType.SLEEP;

    /**
     * If true, reports time spent sleeping as 'cumulativeThrottlingMicros' metric. This enables
     * Dataflow to detect throttled stages, which would influence scaling decisions.
     */
    @JsonProperty public boolean reportThrottlingMicros;

    @Override
    public void validate() {
      super.validate();
      checkArgument(
          outputRecordsPerInputRecord >= 0,
          "outputRecordsPerInputRecord should be a non-negative number, but found %s.",
          outputRecordsPerInputRecord);
      if (maxWorkerThroughput >= 0) {
        checkArgument(
            perBundleDelay == 0,
            "maxWorkerThroughput and perBundleDelay cannot be enabled simultaneously.");
      }
    }
  }

  /**
   * static cache to store one worker level rate limiter for a step. Value in KV is the desired
   * rate.
   */
  private static LoadingCache<KV<Long, Long>, RateLimiter> rateLimiterCache =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<KV<Long, Long>, RateLimiter>() {
                @Override
                public RateLimiter load(KV<Long, Long> pair) {
                  return RateLimiter.create(pair.getValue().doubleValue());
                }
              });
}
