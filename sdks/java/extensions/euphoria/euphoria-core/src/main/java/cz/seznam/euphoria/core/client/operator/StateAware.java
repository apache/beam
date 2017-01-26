/**
 * Copyright 2016 Seznam a.s.
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

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Interface marking operator as state aware.
 * State aware operators are global operators that work on some type of key.
 */
public interface StateAware<IN, KEY> extends PartitioningAware<KEY> {

  /** Retrieve the extractor of key from given element. */
  UnaryFunction<IN, KEY> getKeyExtractor();


}
