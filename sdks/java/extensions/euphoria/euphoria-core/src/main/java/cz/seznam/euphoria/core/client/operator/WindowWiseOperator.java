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
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, W extends Window>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  protected Windowing<WIN, W> windowing;
  protected UnaryFunction<WIN, Long> eventTimeAssigner;

  public WindowWiseOperator(String name,
                            Flow flow,
                            Windowing<WIN, W> windowing /* optional */,
                            UnaryFunction<WIN, Long> eventTimeAssigner /* optional */)
  {
    super(name, flow);
    this.windowing = windowing;
    this.eventTimeAssigner = eventTimeAssigner;
  }

  @Override
  public Windowing<WIN, W> getWindowing() {
    return windowing;
  }

  @Override
  public UnaryFunction<WIN, Long> getEventTimeAssigner() {
    return eventTimeAssigner;
  }
}
