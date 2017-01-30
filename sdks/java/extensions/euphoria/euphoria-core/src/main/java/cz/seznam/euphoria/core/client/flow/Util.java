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
package cz.seznam.euphoria.core.client.flow;

class Util {

  /**
   * Trim string (instead of empty string return null)
   *
   * @param s input string
   * @return non-empty trimmed string or null
   */
  static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.isEmpty()) {
      return null;
    }
    return s;
  }

  private Util() {}
}
