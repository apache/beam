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
package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

/**
 * Single partition of dataset.
 *
 * @param <T> the type of elements this partition hosts, i.e. is able to provide
 */
public interface Partition<T> extends Serializable {

  /**
   * Get location strings (hostnames) of this partition. This is typically
   * utilized when distributing a computation in a clustered environment.
   *
   * @return a collection of hostnames on which this partition is located;
   *          must not return {@code null} but can be the empty set
   */
  Set<String> getLocations();

  /**
   * Opens a reader over this partition. It the caller's
   * responsibility to close the reader once not needed anymore.
   *
   * @return an opened reader to this partition's data
   *
   * @throws IOException if opening a reader to this partitions
   *          data fails for some reason
   */
  Reader<T> openReader() throws IOException;

}
