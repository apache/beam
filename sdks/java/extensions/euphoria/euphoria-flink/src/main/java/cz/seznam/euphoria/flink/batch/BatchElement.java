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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * Single element flowing through Flink batch pipeline. Every such element
 * is associated with a window identifier and timestamp.
 * @param <W> type of the assigned window
 * @param <T> type of the data element
 */
public class BatchElement<W extends Window, T> implements WindowedElement<W, T> {

  private Window window;
  private long timestamp;
  private T element;

  // This class needs to ne POJO for effective serialization
  public BatchElement() {
  }

  public BatchElement(W window, long timestamp, T element) {
    this.window = window;
    this.timestamp = timestamp;
    this.element = element;
  }

  @Override
  @SuppressWarnings("unchecked")
  public W getWindow() {
    return (W) window;
  }

  public void setWindow(W window) {
    this.window = window;
  }

  @Override
  public T getElement() {
    return element;
  }

  public void setElement(T element) {
    this.element = element;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
