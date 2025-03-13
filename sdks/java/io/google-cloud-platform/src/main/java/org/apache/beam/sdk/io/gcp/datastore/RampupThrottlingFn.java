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
package org.apache.beam.sdk.io.gcp.datastore;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also
 * https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottlingFn<T> extends DoFn<T, T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RampupThrottlingFn.class);
  private static final double BASE_BUDGET = 500.0;
  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
  private static final FluentBackoff fluentBackoff = FluentBackoff.DEFAULT;

  private final ValueProvider<Integer> numWorkers;
  private final PCollectionView<Instant> firstInstantSideInput;

  @VisibleForTesting
  Counter throttlingMsecs =
      Metrics.counter(RampupThrottlingFn.class, Metrics.THROTTLE_TIME_COUNTER_NAME);

  // Initialized on every setup.
  private transient MovingFunction successfulOps;

  @VisibleForTesting transient Sleeper sleeper;

  public RampupThrottlingFn(
      ValueProvider<Integer> numWorkers, PCollectionView<Instant> firstInstantSideInput) {
    this.numWorkers = numWorkers;
    this.sleeper = Sleeper.DEFAULT;
    this.successfulOps =
        new MovingFunction(
            Duration.standardSeconds(1).getMillis(),
            Duration.standardSeconds(1).getMillis(),
            1 /* numSignificantBuckets */,
            1 /* numSignificantSamples */,
            Sum.ofLongs());
    this.firstInstantSideInput = firstInstantSideInput;
  }

  public RampupThrottlingFn(int numWorkers, PCollectionView<Instant> timestampSideInput) {
    this(StaticValueProvider.of(numWorkers), timestampSideInput);
  }

  // 500 / numWorkers * 1.5^max(0, (x-5)/5), or "+50% every 5 minutes"
  private int calcMaxOpsBudget(Instant first, Instant instant, int hintNumWorkers) {
    double rampUpIntervalMinutes = (double) RAMP_UP_INTERVAL.getStandardMinutes();
    Duration durationSinceFirst = new Duration(first, instant);

    double calculatedGrowth =
        (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
    double growth = Math.max(0, calculatedGrowth);
    double maxOpsBudget = BASE_BUDGET / hintNumWorkers * Math.pow(1.5, growth);
    return (int) Math.min(Integer.MAX_VALUE, Math.max(1, maxOpsBudget));
  }

  @Setup
  public void setup() {
    this.sleeper = Sleeper.DEFAULT;
    this.successfulOps =
        new MovingFunction(
            Duration.standardSeconds(1).getMillis(),
            Duration.standardSeconds(1).getMillis(),
            1 /* numSignificantBuckets */,
            1 /* numSignificantSamples */,
            Sum.ofLongs());
  }

  /** Emit only as many elements as the exponentially increasing budget allows. */
  @ProcessElement
  public void processElement(ProcessContext c) throws IOException, InterruptedException {
    Instant firstInstant = c.sideInput(firstInstantSideInput);

    T element = c.element();
    BackOff backoff = fluentBackoff.backoff();
    while (true) {
      Instant instant = Instant.now();
      int maxOpsBudget = calcMaxOpsBudget(firstInstant, instant, this.numWorkers.get());
      long currentOpCount = successfulOps.get(instant.getMillis());
      long availableOps = maxOpsBudget - currentOpCount;

      if (maxOpsBudget >= Integer.MAX_VALUE || availableOps > 0) {
        c.output(element);
        successfulOps.add(instant.getMillis(), 1);
        return;
      }

      long backoffMillis = backoff.nextBackOffMillis();
      LOG.info("Delaying by {}ms to conform to gradual ramp-up.", backoffMillis);
      throttlingMsecs.inc(backoffMillis);
      sleeper.sleep(backoffMillis);
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(
        DisplayData.item("hintNumWorkers", numWorkers)
            .withLabel("Number of workers for ramp-up throttling algorithm"));
  }
}
