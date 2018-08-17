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
package cz.seznam.euphoria.fluent;

import cz.seznam.euphoria.core.util.Settings;

/** Helper class providing convenient start points into the fluent api. */
public class Fluent {

  public static Flow flow(String name) {
    return Flow.create(name);
  }

  public static Flow flow(String name, Settings settings) {
    return Flow.create(name, settings);
  }

  public static <T> Dataset<T>
  lift(cz.seznam.euphoria.core.client.dataset.Dataset<T> xs) {
    return new Dataset<>(xs);
  }

}
