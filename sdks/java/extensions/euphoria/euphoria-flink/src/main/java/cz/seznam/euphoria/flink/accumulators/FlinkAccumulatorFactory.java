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
package cz.seznam.euphoria.flink.accumulators;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * Flink specific implementation of {@link AccumulatorProvider.Factory}
 * accepting Flink runtime context as an argument.
 * <p>
 * It is required this factory is thread-safe.
 */
public interface FlinkAccumulatorFactory extends Serializable {

  /**
   * Creates a new instance of {@link AccumulatorProvider}
   * initialized by given settings.
   *
   * @param settings Euphoria settings.
   * @param context  Flink runtime context.
   * @return Instance of accumulator provider.
   */
  AccumulatorProvider create(Settings settings, RuntimeContext context);

  /**
   * Adapts generic euphoria accumulator factory to Flink specific
   * usage.
   */
  class Adapter implements FlinkAccumulatorFactory {

    private final AccumulatorProvider.Factory factory;

    public Adapter(AccumulatorProvider.Factory factory) {
      this.factory = factory;
    }

    @Override
    public AccumulatorProvider create(Settings settings, RuntimeContext context) {
      return factory.create(settings);
    }
  }
}
