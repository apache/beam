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
package org.apache.beam.sdk.fn.stream;

import java.util.Iterator;

/** {@link Iterator} that supports prefetching the next set of records. */
public interface PrefetchableIterator<T> extends Iterator<T> {

  /**
   * Returns {@code true} if and only if {@link #hasNext} and {@link #next} will not require an
   * expensive operation.
   */
  boolean isReady();

  /**
   * If not {@link #isReady}, schedules the next expensive operation such that at some point in time
   * in the future {@link #isReady} will return true.
   */
  void prefetch();
}
