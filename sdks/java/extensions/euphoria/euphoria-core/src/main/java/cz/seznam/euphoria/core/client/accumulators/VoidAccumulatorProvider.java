/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.util.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Placeholder implementation of {@link AccumulatorProvider} that
 * may be used in executors as a default.
 */
@Audience(Audience.Type.EXECUTOR)
public class VoidAccumulatorProvider implements AccumulatorProvider {

  private static final Logger LOG = LoggerFactory.getLogger(VoidAccumulatorProvider.class);

  private VoidAccumulatorProvider() {}

  @Override
  public Counter getCounter(String name) {
    return VoidCounter.INSTANCE;
  }

  @Override
  public Histogram getHistogram(String name) {
    return VoidHistogram.INSTANCE;
  }

  @Override
  public Timer getTimer(String name) {
    return VoidTimer.INSTANCE;
  }

  public static Factory getFactory() {
    return Factory.get();
  }

  // ------------------------

  public static class Factory implements AccumulatorProvider.Factory {

    private static final Factory INSTANCE = new Factory();

    private static final AccumulatorProvider PROVIDER =
            new VoidAccumulatorProvider();

    private static final AtomicBoolean isLogged = new AtomicBoolean();

    private Factory() {}

    public static Factory get() {
      return INSTANCE;
    }

    @Override
    public AccumulatorProvider create(Settings settings) {
      if (isLogged.compareAndSet(false, true)) {
        LOG.warn("Using accumulators with VoidAccumulatorProvider will have no effect");
      }
      return PROVIDER;
    }
  }

  // ------------------------

  private static class VoidCounter implements Counter {

    private static final VoidCounter INSTANCE = new VoidCounter();

    private VoidCounter() {}

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

    private static final VoidHistogram INSTANCE = new VoidHistogram();

    private VoidHistogram() {}

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

    private static final VoidTimer INSTANCE = new VoidTimer();

    private VoidTimer() {}

    @Override
    public void add(Duration duration) {
      // NOOP
    }
  }
}
