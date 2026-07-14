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
package org.apache.beam.runners.core.triggers;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.BitSet;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.sdk.state.ValueState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class TriggerStateMachineRunnerTest {

  @Mock private StateAccessor<?> mockState;
  @Mock private ValueState<BitSet> mockFinishedSetState;
  @Mock private TriggerStateMachineContextFactory<?> mockContextFactory;
  @Mock private TriggerStateMachine mockTriggerStateMachine;

  private ExecutableTriggerStateMachine rootTrigger;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockState.access(TriggerStateMachineRunner.FINISHED_BITS_TAG))
        .thenReturn((ValueState) mockFinishedSetState);
    rootTrigger = ExecutableTriggerStateMachine.create(mockTriggerStateMachine);
  }

  @Test
  public void testPersistFinishedSet_emptyAndOptimizationEnabled() throws Exception {
    when(mockFinishedSetState.read()).thenReturn(null);

    TriggerStateMachineRunner<?> runner =
        new TriggerStateMachineRunner<>(
            rootTrigger,
            (TriggerStateMachineContextFactory) mockContextFactory,
            true /* useNewWindowOptimization */);

    FinishedTriggersBitSet modifiedFinishedSet = FinishedTriggersBitSet.emptyWithCapacity(1);

    runner.persistFinishedSet(mockState, modifiedFinishedSet);

    // Should write empty bitset because optimization is enabled
    verify(mockFinishedSetState).write(modifiedFinishedSet.getBitSet());
  }

  @Test
  public void testPersistFinishedSet_emptyAndOptimizationDisabled() throws Exception {
    when(mockFinishedSetState.read()).thenReturn(null);

    TriggerStateMachineRunner<?> runner =
        new TriggerStateMachineRunner<>(
            rootTrigger,
            (TriggerStateMachineContextFactory) mockContextFactory,
            false /* useNewWindowOptimization */);

    FinishedTriggersBitSet modifiedFinishedSet = FinishedTriggersBitSet.emptyWithCapacity(1);

    runner.persistFinishedSet(mockState, modifiedFinishedSet);

    // Should NOT write empty bitset because optimization is disabled and it was already empty (read
    // returned null)
    verify(mockFinishedSetState, never()).write(modifiedFinishedSet.getBitSet());
  }

  private void runTestPersistFinishedSet_nonEmpty(boolean useNewWindowOptimization)
      throws Exception {
    when(mockFinishedSetState.read()).thenReturn(null);

    TriggerStateMachineRunner<?> runner =
        new TriggerStateMachineRunner<>(
            rootTrigger,
            (TriggerStateMachineContextFactory) mockContextFactory,
            useNewWindowOptimization);

    FinishedTriggersBitSet modifiedFinishedSet = FinishedTriggersBitSet.emptyWithCapacity(1);
    modifiedFinishedSet.setFinished(rootTrigger, true);

    runner.persistFinishedSet(mockState, modifiedFinishedSet);

    // Should write non-empty bitset
    verify(mockFinishedSetState).write(modifiedFinishedSet.getBitSet());
  }

  @Test
  public void testPersistFinishedSet_nonEmpty() throws Exception {
    runTestPersistFinishedSet_nonEmpty(false);
  }

  @Test
  public void testPersistFinishedSet_nonEmptyAndOptimizationEnabled() throws Exception {
    runTestPersistFinishedSet_nonEmpty(true);
  }
}
