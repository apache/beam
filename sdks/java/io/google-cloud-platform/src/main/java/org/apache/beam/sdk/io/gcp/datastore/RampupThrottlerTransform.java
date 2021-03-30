package org.apache.beam.sdk.io.gcp.datastore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import jdk.internal.jline.internal.Log;
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

/**
 * An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
 */
public class RampupThrottlerTransform<T> extends
    PTransform<PCollection<T>, PCollection<T>> implements Serializable {

  private static final double BASE_BUDGET = 500.0;
  private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);

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
        .apply(rewindow)
        .apply(MapElements.via(new SimpleFunction<T, KV<Integer, T>>() {
          @Override
          public KV<Integer, T> apply(T input) {
            int shard_id = (int) (numShards * Math.random());
            return KV.of(shard_id, input);
          }
        }))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new RampupThrottlingFn()))
        .apply(Window.<T>into(new IdentityWindowFn<>(originalWindowFn.windowCoder())));
  }

  class RampupThrottlingFn extends DoFn<KV<Integer, Iterable<T>>, T> implements Serializable {

    private final FluentBackoff fluentBackoff;

    private final transient MovingFunction successfulOps;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    // Is initialized on first operation (e.g., write).
    private Optional<Instant> firstOpInstant = Optional.empty();

    private RampupThrottlingFn() {
      this.fluentBackoff = FluentBackoff.DEFAULT;
      this.successfulOps = new MovingFunction(
          Duration.standardSeconds(1).getMillis(),
          Duration.standardSeconds(1).getMillis(),
          1 /* numSignificantBuckets */,
          1 /* numSignificantSamples */,
          Sum.ofLongs());
    }

    // 500 / numShards * 1.5^max(0, (x-5)/5)
    private int calcMaxOpsBudget(Instant first, Instant instant) {
      long rampUpIntervalMinutes = RAMP_UP_INTERVAL.getStandardMinutes();
      Duration durationSinceFirst = new Duration(instant, first);

      long calculatedGrowth =
          (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
      long growth = Math.max(0, calculatedGrowth);
      double maxRequestCountBudget = BASE_BUDGET / numShards * Math.pow(1.5, growth);
      return (int) maxRequestCountBudget;
    }

    private void recordSuccessfulOps(Instant instant, int numOps) {
      successfulOps.add(instant.getMillis(), numOps);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, InterruptedException {
      Instant instant = Instant.now();
      if (!firstOpInstant.isPresent()) {
        firstOpInstant = Optional.of(instant);
      }

      Iterator<T> elementsIter = c.element().getValue().iterator();
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = fluentBackoff.backoff();
      while (true) {
        int maxOpsBudget = calcMaxOpsBudget(firstOpInstant.get(), instant);
        long currentOpCount = successfulOps.get(instant.getMillis());
        long availableOps = maxOpsBudget - currentOpCount;

        int i = 0;
        while (availableOps > 0 && elementsIter.hasNext()) {
          T element = elementsIter.next();

          c.output(element);
          i++;
          availableOps--;
        }
        successfulOps.add(instant.getMillis(), i);

        if (!elementsIter.hasNext()) {
          break;
        }
        long backoffMillis = backoff.nextBackOffMillis();
        Log.info("Delaying by {} to conform to gradual ramp-up.", backoffMillis);
        sleeper.sleep(backoffMillis);
      }
    }

  }

}
