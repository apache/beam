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
package org.apache.beam.fn.harness;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;

/**
 * Simple state class which collects the totalMillis spent in the state. Allows attaching an
 * arbitrary set of key value labels which can be retrieved later for reporting purposes.
 */
public class SimpleExecutionState extends ExecutionState {
  private long totalMillis = 0;
  private HashMap<String, String> labelsMetadata;

  /**
   * @param name A string name for the state.
   * @param labelsMetadata arbitrary metadata to use for reporting purposes.
   */
  public SimpleExecutionState(String name, HashMap<String, String> labelsMetadata) {
    super(name);
    this.labelsMetadata = labelsMetadata;
    // TODO write unit tests for this class.
  }

  public Map<String, String> getLabels() {
    return Collections.unmodifiableMap(labelsMetadata);
  }

  @Override
  public void takeSample(long millisSinceLastSample) {
    this.totalMillis += millisSinceLastSample;
  }

  public long getTotalMillis() {
    return totalMillis;
  }

  @Override
  public void reportLull(Thread trackedThread, long millis) {
    // TOOD(ajamato): Implement lullz detection to log stuck PTransforms.
  }
}
