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
package org.apache.beam.runners.flink.translation.functions;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

/** Tests for {@link FlinkStatefulDoFnFunction}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class FlinkStatefulDoFnFunctionTest {

  @Test
  public void testAccumulatorRegistrationOnOperatorClose() throws Exception {
    FlinkStatefulDoFnFunction doFnFunction =
        new TestDoFnFunction(
            "step",
            WindowingStrategy.globalDefault(),
            Collections.emptyMap(),
            PipelineOptionsFactory.create(),
            Collections.emptyMap(),
            new TupleTag<>(),
            null,
            Collections.emptyMap(),
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    doFnFunction.open(new Configuration());

    String metricContainerFieldName = "metricContainer";
    FlinkMetricContainer monitoredContainer =
        Mockito.spy(
            (FlinkMetricContainer)
                Whitebox.getInternalState(doFnFunction, metricContainerFieldName));
    Whitebox.setInternalState(doFnFunction, metricContainerFieldName, monitoredContainer);

    doFnFunction.close();
    Mockito.verify(monitoredContainer).registerMetricsForPipelineResult();
  }

  private static class TestDoFnFunction extends FlinkStatefulDoFnFunction {

    public TestDoFnFunction(
        String stepName,
        WindowingStrategy windowingStrategy,
        Map sideInputs,
        PipelineOptions options,
        Map outputMap,
        TupleTag mainOutputTag,
        Coder inputCoder,
        Map outputCoderMap,
        DoFnSchemaInformation doFnSchemaInformation,
        Map sideInputMapping) {
      super(
          new IdentityFn(),
          stepName,
          windowingStrategy,
          sideInputs,
          options,
          outputMap,
          mainOutputTag,
          inputCoder,
          outputCoderMap,
          doFnSchemaInformation,
          sideInputMapping);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
      return Mockito.mock(RuntimeContext.class);
    }

    private static class IdentityFn<T> extends DoFn<T, T> {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(c.element());
      }
    }
  }
}
