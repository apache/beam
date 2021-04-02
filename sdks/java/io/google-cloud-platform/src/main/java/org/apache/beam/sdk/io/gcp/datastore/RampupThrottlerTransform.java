package org.apache.beam.sdk.io.gcp.datastore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottlerTransform<T> extends
    PTransform<PCollection<T>, PCollection<T>> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RampupThrottlerTransform.class);
  private static final double BASE_BUDGET = 500.0;
  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
  private static final FluentBackoff fluentBackoff = FluentBackoff.DEFAULT;

  private final int numShards;

  public RampupThrottlerTransform(int numShards) {
    this.numShards = numShards;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    // We want to reshard the collection to enforce a parallelization limit, but not hold up
    // processing. To do that, we need to set the window to trigger on every element.
    WindowFn<?, ?> originalWindowFn = input.getWindowingStrategy().getWindowFn();
    Window<T> rewindow =
        Window.<T>into(
            new IdentityWindowFn<>(originalWindowFn.windowCoder()))
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    return input
        .apply("Prepare window for sharding", rewindow)
        .apply("Assign random shard keys", MapElements.via(new SimpleFunction<T, KV<Integer, T>>() {
          @Override
          public KV<Integer, T> apply(T input) {
            int shard_id = (int) (numShards * Math.random());
            return KV.of(shard_id, input);
          }
        }))
        .apply("Throttler resharding", GroupByKey.create())
        .apply("Throttle for ramp-up", ParDo.of(new RampupThrottlingFn()))
        .apply("Reset window",
            Window.<T>into(new IdentityWindowFn<>(originalWindowFn.windowCoder())));
  }

  class RampupThrottlingFn extends DoFn<KV<Integer, Iterable<T>>, T> implements Serializable {

    // Initialized on Beam setup.
    private transient MovingFunction successfulOps;
    private Instant firstOpInstant;

    private RampupThrottlingFn() {
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    // 500 / numShards * 1.5^max(0, (x-5)/5)
    private int calcMaxOpsBudget(Instant first, Instant instant) {
      long rampUpIntervalMinutes = RAMP_UP_INTERVAL.getStandardMinutes();
      Duration durationSinceFirst = new Duration(first, instant);

      double calculatedGrowth =
          (1.0 * durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
      double growth = Math.max(0, calculatedGrowth);
      double maxRequestCountBudget = BASE_BUDGET / numShards * Math.pow(1.5, growth);
      return (int) maxRequestCountBudget;
    }

    @Setup
    public void setup() {
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
      this.firstOpInstant = Instant.now();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, InterruptedException {
      Instant nonNullableFirstInstant = firstOpInstant;

      int shard_id = c.element().getKey();
      Iterator<T> elementsIter = c.element().getValue().iterator();
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = fluentBackoff.backoff();
      while (true) {
        Instant instant = Instant.now();
        int maxOpsBudget = calcMaxOpsBudget(nonNullableFirstInstant, instant);
        LOG.info("Shard {}: Max budget is {} entities/s after {}s", shard_id, maxOpsBudget, new Duration(nonNullableFirstInstant, instant).getStandardSeconds());
        long currentOpCount = successfulOps.get(instant.getMillis());
        long availableOps = maxOpsBudget - currentOpCount;

        int i = 0;
        while (availableOps > 0 && elementsIter.hasNext()) {
          T element = elementsIter.next();

          c.output(element);
          backoff.reset();
          i++;
          availableOps--;
        }
        successfulOps.add(instant.getMillis(), i);

        if (!elementsIter.hasNext()) {
          break;
        }
        long backoffMillis = backoff.nextBackOffMillis();
        LOG.info("Delaying by {}ms to conform to gradual ramp-up.", backoffMillis);
        sleeper.sleep(backoffMillis);
      }
    }


  }

}
