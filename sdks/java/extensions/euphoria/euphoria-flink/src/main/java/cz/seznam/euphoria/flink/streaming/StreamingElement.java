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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * Single element flowing through Flink streaming pipeline. Every such element
 * is associated with a window identifier and timestamp.
 * @param <W> type of the assigned window
 * @param <T> type of the data element
 */
public class StreamingElement<W extends Window, T> {

  private Window window;
  private T element;

  // This class needs to ne POJO for effective serialization
  public StreamingElement() {
  }

  public StreamingElement(W window, T element) {
    this.window = window;
    this.element = element;
  }

  @SuppressWarnings("unchecked")
  public W getWindow() {
    return (W) window;
  }

  public void setWindow(W window) {
    this.window = window;
  }

  public T getElement() {
    return element;
  }

  public void setElement(T element) {
    this.element = element;
  }
}
