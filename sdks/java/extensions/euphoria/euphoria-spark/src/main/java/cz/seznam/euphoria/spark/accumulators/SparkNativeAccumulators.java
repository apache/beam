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
package cz.seznam.euphoria.spark.accumulators;

import cz.seznam.euphoria.core.client.accumulators.Accumulator;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Integrates the native Spark accumulators. Accumulators will be
 * visible in the Spark UI.
 */
public class SparkNativeAccumulators implements AccumulatorProvider, Serializable {

  private final SparkAccumulatorHolder accumulators;

  public SparkNativeAccumulators(SparkAccumulatorHolder accumulators) {
    this.accumulators = accumulators;
  }

  @Override
  public Counter getCounter(String name) {
    return accumulators.getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulators.getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    return accumulators.getTimer(name);
  }

  public static class Factory implements SparkAccumulatorFactory {

    private static final Factory INSTANCE = new Factory();

    // ~ since SparkNativeAccumulators is serializable class
    // it can be directly embedded into the factory
    private SparkNativeAccumulators provider;

    private Factory() {}

    public static Factory get() {
      return INSTANCE;
    }

    @Override
    public AccumulatorProvider create(Settings settings) {
      if (provider == null) {
        throw new IllegalStateException("Accumulator factory not initialized.");
      }

      return provider;
    }

    @Override
    public void init(JavaSparkContext sparkContext) {
      SparkAccumulatorHolder sparkAccumulatorHolder = new SparkAccumulatorHolder();
      sparkContext.sc().register(sparkAccumulatorHolder, "accumulators");

      provider = new SparkNativeAccumulators(sparkAccumulatorHolder);
    }
  }

  /**
   * This class contains a map of accumulators. Spark requires all accumulators
   * to be declared before the job is executed. This holder is created before
   * the job is launched and registered to Spark context allowing Euphoria
   * to add accumulators on the fly.
   */
  private static class SparkAccumulatorHolder
          extends AccumulatorV2<Map<String, SparkAccumulator>, Map<String, SparkAccumulator>> {

    private final Map<String, SparkAccumulator> accs;

    public SparkAccumulatorHolder() {
      this.accs = new HashMap<>();
    }

    public SparkAccumulatorHolder(Map<String, SparkAccumulator> accs) {
      this.accs = new HashMap<>(accs);
    }

    @Override
    public boolean isZero() {
      return accs.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<String, SparkAccumulator>, Map<String, SparkAccumulator>> copy() {
      return new SparkAccumulatorHolder(accs);
    }

    @Override
    public void reset() {
      accs.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(Map<String, SparkAccumulator> other) {
      // merge values from both maps by key
      other.forEach((k, v) -> accs.merge(k, v, SparkAccumulator::merge));
    }

    @Override
    public void merge(AccumulatorV2<Map<String, SparkAccumulator>, Map<String, SparkAccumulator>> other) {
      this.add(other.value());
    }

    @Override
    public Map<String, SparkAccumulator> value() {
      return accs;
    }

    public SparkCounter getCounter(String name) {
      return assertType(name, SparkCounter.class, accs.computeIfAbsent(name, s -> new SparkCounter()));
    }

    public SparkHistogram getHistogram(String name) {
      return assertType(name, SparkHistogram.class, accs.computeIfAbsent(name, s -> new SparkHistogram()));
    }

    public SparkTimer getTimer(String name) {
      return assertType(name, SparkTimer.class, accs.computeIfAbsent(name, s -> new SparkTimer()));
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
  }
}
