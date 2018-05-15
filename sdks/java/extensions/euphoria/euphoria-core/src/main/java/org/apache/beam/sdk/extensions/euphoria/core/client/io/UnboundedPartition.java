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

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

import java.io.IOException;
import java.io.Serializable;

/**
 * Single partition of a bounded dataset.
 *
 * @param <T> the type of elements this partition hosts, i.e. is able to provide
 */
@Audience(Audience.Type.CLIENT)
public interface UnboundedPartition<T, OffsetT> extends Serializable {

  /**
   * Opens a reader over this partition. It the caller's responsibility to close the reader once not
   * needed anymore.
   *
   * @return an opened reader to this partition's data
   * @throws IOException if opening a reader to this partitions data fails for some reason
   */
  UnboundedReader<T, OffsetT> openReader() throws IOException;
}
