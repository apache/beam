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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

public class SparkElement<W extends Window, T> implements WindowedElement<W, T> {

  private W window;
  private T element;
  private long timestamp;

  public SparkElement(W window, long timestamp, T element) {
    this.window = window;
    this.element = element;
    this.timestamp = timestamp;
  }

  @Override
  public W getWindow() {
    return window;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public T getElement() {
    return element;
  }
}
