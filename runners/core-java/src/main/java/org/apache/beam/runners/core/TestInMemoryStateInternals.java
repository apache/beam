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
package org.apache.beam.runners.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.joda.time.Instant;

/** Simulates state like {@link InMemoryStateInternals} and provides some extra helper methods. */
public class TestInMemoryStateInternals<K> extends InMemoryStateInternals<K> {
  public TestInMemoryStateInternals(K key) {
    super(key);
  }

  public Set<StateTag> getTagsInUse(StateNamespace namespace) {
    Set<StateTag> inUse = new HashSet<>();
    for (Map.Entry<StateTag, State> entry : inMemoryState.getTagsInUse(namespace).entrySet()) {
      if (!isEmptyForTesting(entry.getValue())) {
        inUse.add(entry.getKey());
      }
    }
    return inUse;
  }

  public Set<StateNamespace> getNamespacesInUse() {
    return inMemoryState.getNamespacesInUse();
  }

  /** Return the earliest output watermark hold in state, or null if none. */
  public Instant earliestWatermarkHold() {
    Instant minimum = null;
    for (State storage : inMemoryState.values()) {
      if (storage instanceof WatermarkHoldState) {
        Instant hold = ((WatermarkHoldState) storage).read();
        if (minimum == null || (hold != null && hold.isBefore(minimum))) {
          minimum = hold;
        }
      }
    }
    return minimum;
  }
}
