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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;

import javax.annotation.Nullable;

/**
 * Operator working on some context.
 */
@Audience(Audience.Type.INTERNAL)
public abstract class WindowWiseOperator<
    IN, WIN, OUT, W extends Window>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  @Nullable
  protected Windowing<WIN, W> windowing;

  public WindowWiseOperator(String name,
                            Flow flow,
                            @Nullable Windowing<WIN, W> windowing) {
    super(name, flow);
    this.windowing = windowing;
  }

  @Nullable
  @Override
  public Windowing<WIN, W> getWindowing() {
    return windowing;
  }
}
