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
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.flink.streaming.ElementProvider;

import java.util.Set;

public final class MultiWindowedElement<WID extends Window, T> implements ElementProvider<T> {
  private final Set<WID> windows;
  private final T element;

  public MultiWindowedElement(Set<WID> windows, T element) {
    this.windows = windows;
    this.element = element;
  }

  @Override
  public T getElement() {
    return element;
  }

  public Set<WID> windows() {
    return windows;
  }

  @Override
  public String toString() {
    return "MultiWindowedElement("
        + windows + ", " + element + ")";
  }


}
