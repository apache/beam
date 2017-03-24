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
import cz.seznam.euphoria.flink.streaming.StreamingElement;

import java.util.Set;

/**
 * Single data element associated with multiple windows. It's rather used
 * for performance optimization as the same thing could be expressed
 * as a set of {@link StreamingElement} instances.
 *
 * @param <WID>   Type of used window
 * @param <KEY>   Type of element's key
 * @param <VALUE> Type of element's data
 */
public class KeyedMultiWindowedElement<WID extends Window, KEY, VALUE> {

  private KEY key;
  private VALUE value;
  private Set<WID> windows;

  public KeyedMultiWindowedElement() {
  }

  public KeyedMultiWindowedElement(KEY key,
                                   VALUE value,
                                   Set<WID> windows) {
    this.key = key;
    this.value = value;
    this.windows = windows;
  }

  public KEY getKey() {
    return key;
  }

  public void setKey(KEY key) {
    this.key = key;
  }

  public VALUE getValue() {
    return value;
  }

  public void setValue(VALUE value) {
    this.value = value;
  }

  public Set<WID> getWindows() {
    return windows;
  }

  public void setWindows(Set<WID> windows) {
    this.windows = windows;
  }
}
