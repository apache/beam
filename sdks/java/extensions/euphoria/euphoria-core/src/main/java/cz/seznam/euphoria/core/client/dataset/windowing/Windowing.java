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
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.Serializable;
import java.util.Set;


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, W extends Window> extends Serializable {

  /**
   * Assign windows to given input element.
   * The element will always have assigned old window, which can be reused
   * by this windowing.
   * The default windowing assigned on input is derived from batch windowing.
   * @param el The element to which windows should be assigned.
   * @return set of windows to be assign this element into, never {@code null}.
   */
  Set<W> assignWindowsToElement(WindowedElement<?, T> el);

  /**
   * Retrieve instance of {@link Trigger} associated with the current windowing
   * strategy.
   */
  Trigger<W> getTrigger();
}
