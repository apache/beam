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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
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

  @Test
  public void testStaticSplitAssignmentByDefault() throws Exception {
    SourceInputFormat<Long> sourceInputFormat =
        new TestSourceInputFormat<>(
            "step", CountingSource.upTo(1000), PipelineOptionsFactory.create());

    SourceInputSplit<Long>[] splits = sourceInputFormat.createInputSplits(2);
    InputSplitAssigner assigner = sourceInputFormat.getInputSplitAssigner(splits);
    assertTrue(assigner instanceof SourceInputFormat.StaticInputSplitAssigner);

    int assigned = 0;
    for (int taskId = 0; taskId < 2; taskId++) {
      InputSplit split;
      while ((split = assigner.getNextInputSplit("host", taskId)) != null) {
        assertEquals(taskId, split.getSplitNumber() % 2);
        assigned++;
      }
    }
    assertEquals(splits.length, assigned);
  }

  @Test
  public void testLazySplitAssignmentWhenRequested() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(FlinkPipelineOptions.class).setSourceStaticSplitThresholdMb(0L);
    SourceInputFormat<Long> sourceInputFormat =
        new TestSourceInputFormat<>("step", CountingSource.upTo(1000), options);

    SourceInputSplit<Long>[] splits = sourceInputFormat.createInputSplits(2);
    assertTrue(
        sourceInputFormat.getInputSplitAssigner(splits) instanceof DefaultInputSplitAssigner);
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
