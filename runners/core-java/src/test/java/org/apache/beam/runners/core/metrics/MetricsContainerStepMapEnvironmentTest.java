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
package org.apache.beam.runners.core.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.Closeable;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link MetricsContainerStepMapEnvironment}. */
public class MetricsContainerStepMapEnvironmentTest {

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private ExecutionState testState() {
    return new ExecutionState("testState") {
      @Override
      public void takeSample(long millisSinceLastSample) {}

      @Override
      public void reportLull(Thread trackedThread, long millis) {}
    };
  }

  @Test
  public void testNoActivatedEnvironmentThrows() {
    thrown.expectMessage("MetricsContainerStepMapEnvironment is not already active");
    MetricsContainerStepMapEnvironment.getCurrent();
  }

  @Test
  public void testActivatedMetricContainerIsReturned() throws Exception {
    try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
      assertThat(MetricsContainerStepMapEnvironment.getCurrent(), is(notNullValue()));
    }
    thrown.expectMessage("MetricsContainerStepMapEnvironment is not already active");
    MetricsContainerStepMapEnvironment.getCurrent();
  }

  @Test
  public void testNoActivatedEnvironmentDisallowsSettingExecutionState() {
    thrown.expectMessage("Cannot enterStateForCurrentTracker if no tracker is active.");
    ExecutionStateTracker.enterStateForCurrentTracker(testState());
  }

  @Test
  public void testActivatedEnvironmentAllowsSettingExecutionState() throws Exception {
    try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
      try (Closeable closeState = ExecutionStateTracker.enterStateForCurrentTracker(testState())) {
        // Intentionally: Do nothing.
      }
    }
    thrown.expectMessage("Cannot enterStateForCurrentTracker if no tracker is active.");
    ExecutionStateTracker.enterStateForCurrentTracker(testState());
  }
}
