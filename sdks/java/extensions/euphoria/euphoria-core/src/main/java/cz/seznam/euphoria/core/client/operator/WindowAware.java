/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator aware of windows.
 *
 * @param <IN> the type of elements processed
 * @param <W> the type of windows handled
 */
public interface WindowAware<IN, W extends Window> {

  Windowing<IN, W> getWindowing();

  /**
   * @return the window time assigner extracts timestamp from given element
   */
  UnaryFunction<IN, Long> getEventTimeAssigner();

}
