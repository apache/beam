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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class SimpleParDoFnHelpersTest {
  private PipelineOptions options;
  @Mock DoFnInstanceManager doFnInstanceManager;
  @Mock SideInputReader sideInputReader;
  @Mock DataflowStepContext stepContext;
  @Mock DataflowStepContext userStepContext;
  @Mock DataflowOperationContext operationContext;
  @Mock DoFnRunnerFactory<String, String> runnerFactory;
  @Mock DoFnRunner<String, String> mockRunner;

  @Mock StreamingSideInputProcessor<String, GlobalWindow> sideInputProcessor;

  @Mock DoFnInfo<String, String> doFnInfo;
  @Mock CounterFactory counterFactory;

  private static class TestDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement() {}
  }

  private TestDoFn doFn = new TestDoFn();

  private SimpleParDoFnHelpers<Object, String, String, GlobalWindow> helpers;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.create();
    when(stepContext.namespacedToUser()).thenReturn(userStepContext);
    when(operationContext.counterFactory()).thenReturn(counterFactory);

    when(doFnInstanceManager.get()).thenReturn((DoFnInfo) doFnInfo);
    when(doFnInfo.getDoFn()).thenReturn(doFn);

    when(runnerFactory.createRunner(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
            any(), any()))
        .thenReturn(mockRunner);

    helpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            new TupleTag<>("main"),
            ImmutableMap.of(new TupleTag<>("main"), 0),
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            runnerFactory,
            k -> {});
  }

  @Test
  public void testReallyStartBundle() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    verify(runnerFactory)
        .createRunner(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
            any(), any());
    verify(mockRunner).startBundle();
  }

  @Test
  public void testFinishBundle() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    helpers.finishBundle(sideInputProcessor);

    verify(mockRunner).finishBundle();
    verify(sideInputProcessor).handleFinishKeyOrBundle();
    verify(doFnInstanceManager).complete(any());
  }

  @Test
  public void testAbort() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    helpers.abort();

    verify(doFnInstanceManager).abort(any());
  }
}
