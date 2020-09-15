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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link BufferingDoFnRunner}.
 *
 * <p>For more tests see:
 *
 * <p>- {@link org.apache.beam.runners.flink.FlinkRequiresStableInputTest}
 *
 * <p>-{@link org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperatorTest}
 *
 * <p>- {@link BufferedElementsTest}
 */
public class BufferingDoFnRunnerTest {

  @Test
  public void testRestoreWithoutConcurrentCheckpoints() throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner = createBufferingDoFnRunner(1, Collections.emptyList());
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(2));
  }

  @Test
  public void testRestoreWithoutConcurrentCheckpointsWithPendingCheckpoint() throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner;

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            1, Collections.singletonList(new BufferingDoFnRunner.CheckpointIdentifier(0, 1000)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(1));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(2));

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            1, Collections.singletonList(new BufferingDoFnRunner.CheckpointIdentifier(1, 1000)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(2));
  }

  @Test
  public void
      testRestoreWithoutConcurrentCheckpointsWithPendingCheckpointFromConcurrentCheckpointing()
          throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner =
        createBufferingDoFnRunner(
            1, Collections.singletonList(new BufferingDoFnRunner.CheckpointIdentifier(5, 42)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(6));
  }

  @Test
  public void testRestoreWithConcurrentCheckpoints() throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner = createBufferingDoFnRunner(2, Collections.emptyList());
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(3));
  }

  @Test
  public void testRestoreWithConcurrentCheckpointsFromPendingCheckpoint() throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner;

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            2, Collections.singletonList(new BufferingDoFnRunner.CheckpointIdentifier(0, 1000)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(1));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(3));

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            2, Collections.singletonList(new BufferingDoFnRunner.CheckpointIdentifier(2, 1000)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(3));
  }

  @Test
  public void testRestoreWithConcurrentCheckpointsFromPendingCheckpoints() throws Exception {
    BufferingDoFnRunner bufferingDoFnRunner;

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            3,
            ImmutableList.of(
                new BufferingDoFnRunner.CheckpointIdentifier(0, 42),
                new BufferingDoFnRunner.CheckpointIdentifier(1, 43)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(2));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(4));

    bufferingDoFnRunner =
        createBufferingDoFnRunner(
            3,
            ImmutableList.of(
                new BufferingDoFnRunner.CheckpointIdentifier(2, 42),
                new BufferingDoFnRunner.CheckpointIdentifier(3, 43)));
    assertThat(bufferingDoFnRunner.currentStateIndex, is(0));
    assertThat(bufferingDoFnRunner.numCheckpointBuffers, is(4));
  }

  @Test
  public void testRejectConcurrentCheckpointingBoundaries() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          createBufferingDoFnRunner(0, Collections.emptyList());
        });
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          createBufferingDoFnRunner(Short.MAX_VALUE, Collections.emptyList());
        });
  }

  private static BufferingDoFnRunner createBufferingDoFnRunner(
      int concurrentCheckpoints,
      List<BufferingDoFnRunner.CheckpointIdentifier> notYetAcknowledgeCheckpoints)
      throws Exception {
    DoFnRunner doFnRunner = Mockito.mock(DoFnRunner.class);
    OperatorStateBackend operatorStateBackend = Mockito.mock(OperatorStateBackend.class);

    // Setup not yet acknowledged checkpoint union list state
    ListState unionListState = Mockito.mock(ListState.class);
    Mockito.when(operatorStateBackend.getUnionListState(Mockito.any())).thenReturn(unionListState);
    Mockito.when(unionListState.get()).thenReturn(notYetAcknowledgeCheckpoints);

    // Setup buffer list state
    Mockito.when(operatorStateBackend.getListState(Mockito.any()))
        .thenReturn(Mockito.mock(ListState.class));

    return BufferingDoFnRunner.create(
        doFnRunner,
        "stable-input",
        StringUtf8Coder.of(),
        WindowedValue.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
        operatorStateBackend,
        null,
        concurrentCheckpoints);
  }
}
