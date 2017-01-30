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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

class IntWindow extends Window implements Comparable<IntWindow> {
  private int val;

  IntWindow(int val) {
    this.val = val;
  }

  public int getValue() {
    return val;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof IntWindow) {
      IntWindow that = (IntWindow) o;
      return this.val == that.val;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return val;
  }


  @Override
  public int compareTo(IntWindow o) {
    return Integer.compare(val, o.val);
  }
}
