/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/** A {@code DataSource} with bounded data. */
@Audience(Audience.Type.EXECUTOR)
public interface BoundedDataSource<T> extends DataSource<T> {

  long SIZE_UNKNOWN = -1L;

  @Override
  default boolean isBounded() {
    return true;
  }

  @Override
  default BoundedDataSource<T> asBounded() {
    return this;
  }

  /**
   * Estimate size of this data source.
   *
   * @return estimated size
   */
  default long sizeEstimate() {
    return SIZE_UNKNOWN;
  }

  /**
   * Split this source to smaller pieces.
   *
   * @param desiredSplitBytes hint of approximately how many bytes each partition should have
   * @return list of partitions covering the original source's content
   */
  List<BoundedDataSource<T>> split(long desiredSplitBytes);

  /**
   * Get location strings (hostnames) of this partition. This is typically utilized when
   * distributing a computation in a clustered environment.
   *
   * @return a collection of hostnames on which this partition is located; must not return {@code
   *     null} but can be the empty set
   */
  Set<String> getLocations();

  /**
   * Opens a reader over this partition. It the caller's responsibility to close the reader once not
   * needed anymore.
   *
   * @return an opened reader to this partition's data
   * @throws IOException if opening a reader to this partitions data fails for some reason
   */
  BoundedReader<T> openReader() throws IOException;
}
