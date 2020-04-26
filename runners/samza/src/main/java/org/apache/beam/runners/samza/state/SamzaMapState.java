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
package org.apache.beam.runners.samza.state;

import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;

/** Samza's extended MapState, allowing extra access methods to the state. */
public interface SamzaMapState<KeyT, ValueT> extends MapState<KeyT, ValueT> {

  /**
   * Returns an iterator from the current map state. Note this is different from the iterable
   * implementation in {@link MapState#entries()}}, where we load the entries into memory and return
   * iterable from that. To handle large state that doesn't fit in memory, we also need this method
   * so it's possible to iterate on large data set and close the iterator when not needed.
   *
   * @return a {@link ReadableState} of an iterator
   */
  ReadableState<Iterator<Map.Entry<KeyT, ValueT>>> readIterator();
}
