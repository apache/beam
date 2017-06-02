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

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark specific implementation of {@link AccumulatorProvider.Factory}
 * having possibility to be initialized with Spark context.
 */
public interface SparkAccumulatorFactory extends AccumulatorProvider.Factory {

  /**
   * Creates a new instance of {@link AccumulatorProvider}
   * initialized by given settings.
   *
   * @param settings Euphoria settings.
   * @return Instance of accumulator provider.
   */
  AccumulatorProvider create(Settings settings);

  /**
   * Opportunity to initialize the factory or underlying accumulator
   * provider during flow execution start. Typically called from
   * Spark driver.
   *
   * @param sparkContext instance of Spark context
   */
  void init(JavaSparkContext sparkContext);

  /**
   * Adapts generic euphoria accumulator factory to Spark specific
   * use case.
   */
  class Adapter implements SparkAccumulatorFactory {

    private final AccumulatorProvider.Factory factory;

    public Adapter(AccumulatorProvider.Factory factory) {
      this.factory = factory;
    }

    @Override
    public AccumulatorProvider create(Settings settings) {
      return factory.create(settings);
    }

    @Override
    public void init(JavaSparkContext sparkContext) {
      // NOOP
    }
  }
}
