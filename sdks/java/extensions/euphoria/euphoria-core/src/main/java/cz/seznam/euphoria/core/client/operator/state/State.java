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
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.Collector;

/** A state for stateful operations. */
@Audience(Audience.Type.CLIENT)
public interface State<IN, OUT> {

  /**
   * Add element to this state.
   *
   * @param element the element to add/accumulate to this state
   */
  void add(IN element);

  /**
   * Flush the state to output. Invoked when window this state is part of gets disposed/triggered.
   *
   * @param context the context to utilize for emitting output elements; never {@code null}
   */
  void flush(Collector<OUT> context);

  /**
   * Closes this state. Invoked after {@link #flush(Collector)} and before this state gets disposed
   * to allow clean-up of temporary state storage.
   */
  void close();
}
