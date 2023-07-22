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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link SourceOperationExecutorFactory}. */
@RunWith(JUnit4.class)
public class SourceOperationExecutorFactoryTest {

  @Mock public DataflowExecutionContext<?> executionContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreateDefault() throws Exception {
    SourceOperationRequest request =
        new SourceOperationRequest()
            .setName("name")
            .setOriginalName("original")
            .setSystemName("system")
            .setStageName("stage")
            .setSplit(new SourceSplitRequest());
    DataflowOperationContext mockOperation = Mockito.mock(DataflowOperationContext.class);
    Mockito.when(executionContext.createOperationContext(Mockito.isA(NameContext.class)))
        .thenReturn(mockOperation);
    SourceOperationExecutor sourceOperationExecutor =
        SourceOperationExecutorFactory.create(
            PipelineOptionsFactory.create(), request, null, executionContext, "STAGE");
    assertThat(sourceOperationExecutor, instanceOf(WorkerCustomSourceOperationExecutor.class));
  }
}
