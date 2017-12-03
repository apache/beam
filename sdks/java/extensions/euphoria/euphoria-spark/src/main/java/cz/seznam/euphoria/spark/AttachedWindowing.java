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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.util.Collections;

class AttachedWindowing<T, W extends Window<W>> implements Windowing<T, W> {

  static final AttachedWindowing INSTANCE = new AttachedWindowing();

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<W> assignWindowsToElement(WindowedElement<?, T> input) {
    return Collections.singleton((W) input.getWindow());
  }

  @Override
  public Trigger<W> getTrigger() {
    return NoopTrigger.get();
  }
}
