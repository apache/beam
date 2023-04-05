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
package org.apache.beam.runners.dataflow.worker.graph;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParallelInstruction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OperationNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OutputReceiverNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Nodes}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class NodesTest {
  private static final String PCOLLECTION_ID = "fakeId";

  @Test
  public void testParallelInstructionNode() {
    ParallelInstruction param = new ParallelInstruction();
    Nodes.ExecutionLocation location = Nodes.ExecutionLocation.UNKNOWN;
    assertSame(param, ParallelInstructionNode.create(param, location).getParallelInstruction());
    assertSame(
        Nodes.ExecutionLocation.UNKNOWN,
        ParallelInstructionNode.create(param, location).getExecutionLocation());
    assertNotEquals(
        ParallelInstructionNode.create(param, location),
        ParallelInstructionNode.create(param, location));
  }

  @Test
  public void testInstructionOutputNode() {
    InstructionOutput param = new InstructionOutput();
    assertSame(param, InstructionOutputNode.create(param, PCOLLECTION_ID).getInstructionOutput());
    assertNotEquals(
        InstructionOutputNode.create(param, PCOLLECTION_ID),
        InstructionOutputNode.create(param, PCOLLECTION_ID));
  }

  @Test
  public void testOutputReceiverNode() {
    OutputReceiver receiver = new OutputReceiver();
    Coder<?> coder = StringUtf8Coder.of();
    assertSame(
        receiver, OutputReceiverNode.create(receiver, coder, PCOLLECTION_ID).getOutputReceiver());
    assertSame(coder, OutputReceiverNode.create(receiver, coder, PCOLLECTION_ID).getCoder());
    assertNotEquals(
        OutputReceiverNode.create(receiver, coder, PCOLLECTION_ID),
        OutputReceiverNode.create(receiver, coder, PCOLLECTION_ID));
  }

  @Test
  public void testOperationNode() {
    Operation param = mock(Operation.class);
    assertSame(param, OperationNode.create(param).getOperation());
    assertNotEquals(OperationNode.create(param), OperationNode.create(param));
  }
}
