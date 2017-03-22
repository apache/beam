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


public class WindowedElementImpl<W extends Window, T> implements WindowedElement<W, T> {

  final T element;
  W window;
  long timestamp;

  public WindowedElementImpl(W window, long timestamp, T element) {
    this.window = window;
    this.timestamp = timestamp;
    this.element = element;
  }

  @Override
  public W getWindow() {
    return window;
  }

  @Override
  public void setWindow(W window) {
    this.window = window;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public T getElement() {
    return element;
  }

  @Override
  public String toString() {
    return "WindowedElement{" +
            "window=" + window +
            ", timestamp=" + timestamp +
            ", element=" + element +
            '}';
  }
}
