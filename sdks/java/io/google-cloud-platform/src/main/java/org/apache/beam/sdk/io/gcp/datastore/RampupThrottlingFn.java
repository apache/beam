package org.apache.beam.sdk.io.gcp.datastore;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottlingFn<T> extends DoFn<T, T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RampupThrottlingFn.class);
  private static final double BASE_BUDGET = 500.0;
  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
  private static final FluentBackoff fluentBackoff = FluentBackoff.DEFAULT;

  private final int numWorkers;
  private final Counter throttlingMsecs =
      Metrics.counter(RampupThrottlingFn.class, "throttling-msecs");

  // Initialized on Beam setup.
  private transient MovingFunction successfulOps;
  private Instant firstInstant;

  @VisibleForTesting transient Sleeper sleeper;

  public RampupThrottlingFn(int numWorkers) {
    this.numWorkers = numWorkers;
    this.sleeper = Sleeper.DEFAULT;
    this.successfulOps = new MovingFunction(
        Duration.standardSeconds(1).getMillis(),
        Duration.standardSeconds(1).getMillis(),
        1 /* numSignificantBuckets */,
        1 /* numSignificantSamples */,
        Sum.ofLongs());
    this.firstInstant = Instant.now();
  }

  // 500 / numWorkers * 1.5^max(0, (x-5)/5)
  private int calcMaxOpsBudget(Instant first, Instant instant) {
    double rampUpIntervalMinutes = (double) RAMP_UP_INTERVAL.getStandardMinutes();
    Duration durationSinceFirst = new Duration(first, instant);

    double calculatedGrowth = (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes)
        / rampUpIntervalMinutes;
    double growth = Math.max(0, calculatedGrowth);
    double maxOpsBudget = BASE_BUDGET / this.numWorkers * Math.pow(1.5, growth);
    return (int) maxOpsBudget;
  }

  @Setup
  public void setup() {
    this.sleeper = Sleeper.DEFAULT;
    this.successfulOps = new MovingFunction(
        Duration.standardSeconds(1).getMillis(),
        Duration.standardSeconds(1).getMillis(),
        1 /* numSignificantBuckets */,
        1 /* numSignificantSamples */,
        Sum.ofLongs());
    this.firstInstant = Instant.now();
  }

  /** Emit only as many elements as the exponentially increasing budget allows. */
  @ProcessElement
  public void processElement(ProcessContext c) throws IOException, InterruptedException {
    Instant nonNullableFirstInstant = firstInstant;

    T element = c.element();
    BackOff backoff = fluentBackoff.backoff();
    while (true) {
      Instant instant = Instant.now();
      int maxOpsBudget = calcMaxOpsBudget(nonNullableFirstInstant, instant);
      long currentOpCount = successfulOps.get(instant.getMillis());
      long availableOps = maxOpsBudget - currentOpCount;

      if (availableOps > 0) {
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


}

