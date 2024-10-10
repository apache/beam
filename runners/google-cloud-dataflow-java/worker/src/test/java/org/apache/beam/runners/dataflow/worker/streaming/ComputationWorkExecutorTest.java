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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowWorkExecutor;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComputationWorkExecutorTest {

  private final DataflowWorkExecutor dataflowWorkExecutor = mock(DataflowWorkExecutor.class);
  private final StreamingModeExecutionContext context = mock(StreamingModeExecutionContext.class);
  private ComputationWorkExecutor computationWorkExecutor;

  @Before
  public void setUp() {
    computationWorkExecutor =
        ComputationWorkExecutor.builder()
            .setWorkExecutor(dataflowWorkExecutor)
            .setContext(context)
            .setExecutionStateTracker(mock(ExecutionStateTracker.class))
            .build();
  }

  @Test
  public void testInvalidate_withoutCallToStart() {
    // Call to invalidate w/o a call to start should not fail.
    computationWorkExecutor.invalidate();
  }

  @Test
  public void testInvalidate_handlesException() {
    AtomicBoolean verifyContextInvalidated = new AtomicBoolean(false);
    Throwable e = new RuntimeException("something bad happened 2");
    doThrow(e).when(dataflowWorkExecutor).close();
    doAnswer(
            ignored -> {
              verifyContextInvalidated.set(true);
              return null;
            })
        .when(context)
        .invalidateCache();
    computationWorkExecutor.invalidate();
    assertTrue(verifyContextInvalidated.get());
  }
}
