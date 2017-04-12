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

import java.io.Serializable;

/**
 * A provider of storage instances.
 */
public interface StorageProvider extends Serializable {

  /**
   * Retrieve new instance of state storage for values of given type.
   *
   * @param <T> the type of values referred to through the descriptor
   *
   * @param descriptor descriptor of the storage within scope of given key and window/operator
   *
   * @return the descriptor of a single value
   */
  <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor);

  /**
   * Retrieve new instance of state storage for lists of values of given type.
   *
   * @param <T> the type of values referred to through the descriptor
   *
   * @param descriptor descriptor of the storage
   *
   * @return the descriptor of a single value
   */
  <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor);

}
