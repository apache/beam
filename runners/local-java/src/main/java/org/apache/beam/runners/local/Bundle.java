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

package org.apache.beam.runners.local;

import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/** An immutable collection of elements which are part of a {@code PCollection}. */
public interface Bundle<T> extends Iterable<WindowedValue<T>> {
  /**
   * Return the minimum timestamp among elements in this bundle.
   *
   * <p>This should be equivalent to iterating over all of the elements within a bundle and
   * selecting the minimum timestamp from among them.
   */
  Instant getMinimumTimestamp();
}
