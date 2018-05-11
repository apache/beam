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
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.Collector;
import java.io.Serializable;

/**
 * Functor of single argument. Functor can produce zero or more elements in return to a call, for
 * which it uses a collector.
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
public interface UnaryFunctor<IN, OUT> extends Serializable {

  /**
   * Applies function to given element.
   *
   * @param elem Input element.
   * @param collector Collector to emit results.
   */
  void apply(IN elem, Collector<OUT> collector);
}
