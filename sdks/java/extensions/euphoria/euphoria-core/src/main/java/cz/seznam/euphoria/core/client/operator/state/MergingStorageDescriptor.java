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
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;

/**
 * Optional interface for descriptor to implement in order to enable state merging.
 *
 * @param <T> the type of elements referred to through this descriptor
 */
public interface MergingStorageDescriptor<T> {

  /**
   * @return the merging function for state storages.
   */
  BinaryFunction<? extends Storage<T>, ? extends Storage<T>, Void> getMerger();

}
