/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.accumulators;

import cz.seznam.euphoria.core.client.accumulators.Accumulator;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.time.Duration;
import java.util.TreeMap;

/**
 * Integrates Flink native accumulator API.
 */
public class FlinkNativeAccumulators implements AccumulatorProvider {

  private final RuntimeContext context;

  private FlinkNativeAccumulators(RuntimeContext context) {
    this.context = context;
  }

  @Override
  public Counter getCounter(String name) {
    return getByNameAndClass(name, FlinkCounter.class);
  }

  @Override
  public Histogram getHistogram(String name) {
    return getByNameAndClass(name, FlinkHistogram.class);
  }

  @Override
  public Timer getTimer(String name) {
    return getByNameAndClass(name, FlinkTimer.class);
  }

  @SuppressWarnings("unchecked")
  private <ACC extends Accumulator> ACC getByNameAndClass(String name,
                                                          Class<ACC> clz) {
    Accumulator acc = (Accumulator) context.getAccumulator(name);

    if (acc == null) {
      try {
        // register a new instance
        acc = clz.getConstructor().newInstance();
        context.addAccumulator(name, (org.apache.flink.api.common.accumulators.Accumulator) acc);
      } catch (Exception e) {
        throw new RuntimeException("Exception during accumulator initialization: " + clz, e);
      }
    }

    if (!clz.equals(acc.getClass())) {
      throw new IllegalStateException(
              "Accumulator named '" + name + "' is type of " + acc.getClass().getSimpleName());
    }

    return (ACC) acc;
  }

  public static Factory getFactory() {
    return Factory.get();
  }

  // ------------------------------

  public static class Factory implements FlinkAccumulatorFactory {

    private static final Factory INSTANCE = new Factory();

    private Factory() {}

    @Override
    public AccumulatorProvider create(Settings settings, RuntimeContext context) {
      return new FlinkNativeAccumulators(context);
    }

    public static Factory get() {
      return INSTANCE;
    }
  }

  // ------------------------------

  // Each class implements both Flink Accumulator and Euphoria Accumulator
  // so it can be used in both contexts without creating multiple instances.

  public static class FlinkCounter extends LongCounter implements Counter {

    @Override
    public void increment(long value) {
      super.add(value);
    }

    @Override
    public void increment() {
      super.add(1L);
    }

    // It is necessary to override clone method to instantiate this
    // extended class and not the parent type.
    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public LongCounter clone() {
      FlinkCounter result = new FlinkCounter();
      result.merge(this);
      return result;
    }
  }

  public static class FlinkHistogram extends org.apache.flink.api.common.accumulators.Histogram
          implements Histogram {

    @Override
    public void add(long value) {
      super.add(Math.toIntExact(value));
    }

    @Override
    public void add(long value, long times) {
      for (int i = 0; i < times; i++) {
        this.add(value);
      }
    }

    // It is necessary to override clone method to instantiate this
    // extended class and not the parent type.
    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public org.apache.flink.api.common.accumulators.Accumulator<Integer, TreeMap<Integer, Integer>> clone() {
      FlinkHistogram result = new FlinkHistogram();
      result.merge(this);
      return result;
    }
  }

  public static class FlinkTimer extends org.apache.flink.api.common.accumulators.Histogram
          implements Timer {

    @Override
    public void add(Duration duration) {
      super.add(Math.toIntExact(duration.toMillis()));
    }

    // It is necessary to override clone method to instantiate this
    // extended class and not the parent type.
    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public org.apache.flink.api.common.accumulators.Accumulator<Integer, TreeMap<Integer, Integer>> clone() {
      FlinkTimer result = new FlinkTimer();
      result.merge(this);
      return result;
    }
  }
}
