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
package cz.seznam.euphoria.operator.test.accumulators;

import cz.seznam.euphoria.core.client.accumulators.Accumulator;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.util.Settings;

import java.io.ObjectStreamException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An accumulator provider gathering accumulators in-memory.<p>
 *
 * Safe for use for unit testing purposes.
 */
public class SingleJvmAccumulatorProvider implements AccumulatorProvider {

  private static final SingleJvmAccumulatorProvider INSTANCE
      = new SingleJvmAccumulatorProvider();

  private SingleJvmAccumulatorProvider() {}

  private final ConcurrentMap<String, Accumulator> accs = new ConcurrentHashMap<>();

  @Override
  public cz.seznam.euphoria.core.client.accumulators.Counter getCounter(String name) {
    return assertType(name, Counter.class, accs.computeIfAbsent(name, s -> new Counter()));
  }

  @Override
  public cz.seznam.euphoria.core.client.accumulators.Histogram getHistogram(String name) {
    return assertType(name, Histogram.class, accs.computeIfAbsent(name, s -> new Histogram()));
  }

  @Override
  public cz.seznam.euphoria.core.client.accumulators.Timer getTimer(String name) {
    return assertType(name, Timer.class, accs.computeIfAbsent(name, s -> new Timer()));
  }

  @SuppressWarnings("unchecked")
  private static <T> T assertType(String name, Class<T> expectedType, Accumulator actualAcc) {
    if (actualAcc.getClass() != expectedType) {
      // ~ provide a nice message (that's why we don't simply use `expectedType.cast(..)`)
      throw new IllegalStateException("Ambiguously named accumulators! Got "
          + actualAcc.getClass() + " for "
          + name + " but expected " + expectedType + "!");
    }
    return (T) actualAcc;
  }

  void clear() {
    accs.clear();
  }

  <V, T extends GetSnapshot<V>> Map<String, V> getSnapshots(Class<T> type) {
    HashMap<String, V> m = new HashMap<>();
    accs.forEach((name, accumulator) -> {
      if (type.isAssignableFrom(accumulator.getClass())) {
        @SuppressWarnings("unchecked")
        T acc = (T) accumulator;
        m.put(name, acc.getSnapshot());
      }
    });
    return m;
  }

  // ~ -----------------------------------------------------------------------

  public static final class Factory implements
      AccumulatorProvider.Factory, SnapshotProvider {

    private static final Factory INSTANCE = new Factory();

    private Factory() {}

    public static Factory get() {
      return INSTANCE;
    }

    public void clear() {
      providerInstance().clear();
    }

    @Override
    public Map<String, Long> getCounterSnapshots() {
      return providerInstance().getSnapshots(Counter.class);
    }

    @Override
    public Map<String, Map<Long, Long>> getHistogramSnapshots() {
      return providerInstance().getSnapshots(Histogram.class);
    }

    @Override
    public Map<String, Map<Duration, Long>> getTimerSnapshots() {
      return providerInstance().getSnapshots(Timer.class);
    }

    @Override
    public AccumulatorProvider create(Settings settings) {
      return providerInstance();
    }

    private SingleJvmAccumulatorProvider providerInstance() {
      return SingleJvmAccumulatorProvider.INSTANCE;
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  }
}
