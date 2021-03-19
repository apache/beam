package org.apache.beam.sdk.io.gcp.datastore;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.MovingFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottler implements Serializable {

  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);

  private final int baseBatchBudget;
  private final transient MovingFunction successfulOps;

  // Is initialized on first operation (e.g., write).
  private @Nullable Instant firstOpInstant = null;

  public RampupThrottler(int baseBatchBudget) {
    this.baseBatchBudget = baseBatchBudget;
    successfulOps = new MovingFunction(
        Duration.standardSeconds(1).getMillis(),
        Duration.standardSeconds(1).getMillis(),
        1 /* numSignificantBuckets */,
        1 /* numSignificantSamples */,
        Sum.ofLongs());
  }

  // 500 * 1.5^max(0, (x-5)/5)
  private int calcMaxOpsBudget(Instant first, Instant instant) {
    long rampUpIntervalMinutes = RAMP_UP_INTERVAL.getStandardMinutes();
    Duration durationSinceFirst = new Duration(instant, first);

    long calculatedGrowth =
        (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
    long growth = Math.max(0, calculatedGrowth);
    double maxRequestCountBudget = baseBatchBudget * Math.pow(1.5, growth);
    return (int) maxRequestCountBudget;
  }

  public void recordSuccessfulOps(Instant instant, int numOps) {
    successfulOps.add(instant.getMillis(), numOps);
  }

  /**
   * Call this before sending a request to the remote service; if this returns true, drop the
   * request (treating it as a failure or trying it again at a later time).
   */
  public boolean throttleRequest(Instant instant) {
    if (firstOpInstant == null) {
      firstOpInstant = instant;
    }

    int maxOpsBudget = calcMaxOpsBudget(firstOpInstant.toInstant(), instant);
    long currentOpCount = successfulOps.get(instant.getMillis());
    long availableOps = maxOpsBudget - currentOpCount;
    return availableOps <= 0;
  }

}
