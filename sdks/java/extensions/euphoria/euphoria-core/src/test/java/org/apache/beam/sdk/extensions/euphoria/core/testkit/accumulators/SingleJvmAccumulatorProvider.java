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
package org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators;

import java.io.ObjectStreamException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Accumulator;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;

/**
 * An accumulator provider gathering accumulators in-memory.
 *
 * <p>Safe for use for unit testing purposes.
 */
public class SingleJvmAccumulatorProvider implements AccumulatorProvider {

  private static final SingleJvmAccumulatorProvider INSTANCE = new SingleJvmAccumulatorProvider();
  private final ConcurrentMap<String, Accumulator> accs = new ConcurrentHashMap<>();

  private SingleJvmAccumulatorProvider() {}

  @SuppressWarnings("unchecked")
  private static <T> T assertType(String name, Class<T> expectedType, Accumulator actualAcc) {
    if (actualAcc.getClass() != expectedType) {
      // ~ provide a nice message (that's why we don't simply use `expectedType.cast(..)`)
      throw new IllegalStateException(
          "Ambiguously named accumulators! Got "
              + actualAcc.getClass()
              + " for "
              + name
              + " but expected "
              + expectedType
              + "!");
    }
    return (T) actualAcc;
  }

  @Override
  public Counter getCounter(String name) {
    return assertType(name, LongCounter.class, accs.computeIfAbsent(name, s -> new LongCounter()));
  }

  @Override
  public Histogram getHistogram(String name) {
    return assertType(
        name, LongHistogram.class, accs.computeIfAbsent(name, s -> new LongHistogram()));
  }

  @Override
  public Timer getTimer(String name) {
    return assertType(
        name, NanosecondTimer.class, accs.computeIfAbsent(name, s -> new NanosecondTimer()));
  }

  void clear() {
    accs.clear();
  }

  <V, T extends Snapshotable<V>> Map<String, V> getSnapshots(Class<T> type) {
    HashMap<String, V> m = new HashMap<>();
    accs.forEach(
        (name, accumulator) -> {
          if (type.isAssignableFrom(accumulator.getClass())) {
            @SuppressWarnings("unchecked")
            T acc = (T) accumulator;
            m.put(name, acc.getSnapshot());
          }
        });
    return m;
  }

  // ~ -----------------------------------------------------------------------

  /** Accumulator provider factory. */
  public static final class Factory implements AccumulatorProvider.Factory, SnapshotProvider {

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
      return providerInstance().getSnapshots(LongCounter.class);
    }

    @Override
    public Map<String, Map<Long, Long>> getHistogramSnapshots() {
      return providerInstance().getSnapshots(LongHistogram.class);
    }

    @Override
    public Map<String, Map<Duration, Long>> getTimerSnapshots() {
      return providerInstance().getSnapshots(NanosecondTimer.class);
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
