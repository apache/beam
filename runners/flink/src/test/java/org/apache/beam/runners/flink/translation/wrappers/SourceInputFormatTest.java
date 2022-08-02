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
package org.apache.beam.runners.flink.translation.wrappers;

import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

/** Tests for {@link SourceInputFormat}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class SourceInputFormatTest {

  @Test
  public void testAccumulatorRegistrationOnOperatorClose() throws Exception {
    SourceInputFormat<Long> sourceInputFormat =
        new TestSourceInputFormat<>(
            "step", CountingSource.upTo(10), PipelineOptionsFactory.create());

    sourceInputFormat.open(sourceInputFormat.createInputSplits(1)[0]);

    String metricContainerFieldName = "metricContainer";
    FlinkMetricContainer monitoredContainer =
        Mockito.spy(
            (FlinkMetricContainer)
                Whitebox.getInternalState(sourceInputFormat, metricContainerFieldName));
    Whitebox.setInternalState(sourceInputFormat, metricContainerFieldName, monitoredContainer);

    sourceInputFormat.close();
    Mockito.verify(monitoredContainer).registerMetricsForPipelineResult();
  }

  private static class TestSourceInputFormat<T> extends SourceInputFormat<T> {

    public TestSourceInputFormat(
        String stepName, BoundedSource initialSource, PipelineOptions options) {
      super(stepName, initialSource, options);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
      return Mockito.mock(RuntimeContext.class);
    }
  }
}
