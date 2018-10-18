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
package org.apache.beam.runners.dataflow.worker.counters;

import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * An observer that gets notified when additional bytes are read and/or used. It adds all bytes into
 * a local counter. When the observer gets advanced via the next() call, it adds the total byte
 * count to the specified counter, and prepares for the next element.
 */
public class CounterBackedElementByteSizeObserver extends ElementByteSizeObserver {
  private final Counter<Long, ?> counter;

  public CounterBackedElementByteSizeObserver(Counter<Long, ?> counter) {
    this.counter = counter;
  }

  @Override
  protected void reportElementSize(long elementByteSize) {
    counter.addValue(elementByteSize);
  }
}
