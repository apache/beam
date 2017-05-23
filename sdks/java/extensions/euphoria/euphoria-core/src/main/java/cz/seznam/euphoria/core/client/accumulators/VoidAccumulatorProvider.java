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
package cz.seznam.euphoria.core.client.accumulators;

import cz.seznam.euphoria.core.util.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Placeholder implementation of {@link AccumulatorProvider} that
 * may be used in executors as a default.
 */
public class VoidAccumulatorProvider implements AccumulatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(VoidAccumulatorProvider.class);

  private final Map<String, Accumulator> accumulators = new HashMap<>();

  private VoidAccumulatorProvider() {}

  @Override
  public Counter getCounter(String name) {
    return getAccumulator(name, VoidCounter.class);
  }

  @Override
  public Histogram getHistogram(String name) {
    return getAccumulator(name, VoidHistogram.class);
  }

  @Override
  public Timer getTimer(String name) {
    return getAccumulator(name, VoidTimer.class);
  }

  private <ACC extends Accumulator> ACC getAccumulator(String name, Class<ACC> clz) {
    try {
      ACC acc = clz.getConstructor().newInstance();
      if (accumulators.putIfAbsent(name, acc) == null) {
        LOG.warn("Using accumulators with VoidAccumulatorProvider will have no effect");
      }
      return acc;
    } catch (Exception e) {
      throw new RuntimeException("Exception during accumulator initialization: " + clz, e);
    }
  }

  public static Factory getFactory() {
    return Factory.get();
  }

  // ------------------------

  public static class Factory implements AccumulatorProvider.Factory {

    private static final Factory INSTANCE = new Factory();

    private static final AccumulatorProvider PROVIDER =
            new VoidAccumulatorProvider();

    private Factory() {}

    @Override
    public AccumulatorProvider create(Settings settings) {
      return PROVIDER;
    }

    public static Factory get() {
      return INSTANCE;
    }
  }

  // ------------------------

  public static class VoidCounter implements Counter {

    @Override
    public void increment(long value) {
      // NOOP
    }

    @Override
    public void increment() {
      // NOOP
    }
  }

  public static class VoidHistogram implements Histogram {

    @Override
    public void add(long value) {
      // NOOP
    }

    @Override
    public void add(long value, long times) {
      // NOOP
    }
  }

  public static class VoidTimer implements Timer {

    @Override
    public void add(Duration duration) {
      // NOOP
    }
  }
}
