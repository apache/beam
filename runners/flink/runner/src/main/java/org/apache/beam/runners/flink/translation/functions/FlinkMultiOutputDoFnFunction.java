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
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Encapsulates a {@link DoFn} that can emit to multiple
 * outputs inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 *
 * <p>We get a mapping from {@link org.apache.beam.sdk.values.TupleTag} to output index
 * and must tag all outputs with the output number. Afterwards a filter will filter out
 * those elements that are not to be in a specific output.
 */
public class FlinkMultiOutputDoFnFunction<InputT, OutputT>
    extends RichMapPartitionFunction<WindowedValue<InputT>, WindowedValue<RawUnionValue>> {

  private final DoFn<InputT, OutputT> doFn;
  private final SerializedPipelineOptions serializedOptions;

  private final Map<TupleTag<?>, Integer> outputMap;

  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private TupleTag<OutputT> mainOutputTag;
  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  public FlinkMultiOutputDoFnFunction(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options,
      Map<TupleTag<?>, Integer> outputMap,
      TupleTag<OutputT> mainOutputTag) {
    this.doFn = doFn;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.outputMap = outputMap;

    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> values,
      Collector<WindowedValue<RawUnionValue>> out) throws Exception {

    RuntimeContext runtimeContext = getRuntimeContext();

    DoFnRunner<InputT, OutputT> doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.getPipelineOptions(), doFn,
        new FlinkSideInputReader(sideInputs, runtimeContext),
        new DoFnOutputManager(out),
        mainOutputTag,
        // see SimpleDoFnRunner, just use it to limit number of side outputs
        Collections.<TupleTag<?>>emptyList(),
        new FlinkNoOpStepContext(),
        new FlinkAggregatorFactory(runtimeContext),
        windowingStrategy);

    doFnRunner.startBundle();

    for (WindowedValue<InputT> value : values) {
      doFnRunner.processElement(value);
    }

    doFnRunner.finishBundle();

  }

  @Override
  public void open(Configuration parameters) throws Exception {
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();
  }

  @Override
  public void close() throws Exception {
    doFnInvoker.invokeTeardown();
  }

  private class DoFnOutputManager
      implements DoFnRunners.OutputManager {

    private Collector<WindowedValue<RawUnionValue>> collector;

    DoFnOutputManager(Collector<WindowedValue<RawUnionValue>> collector) {
      this.collector = collector;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      collector.collect(WindowedValue.of(new RawUnionValue(outputMap.get(tag), output.getValue()),
          output.getTimestamp(), output.getWindows(), output.getPane()));
    }
  }

}
