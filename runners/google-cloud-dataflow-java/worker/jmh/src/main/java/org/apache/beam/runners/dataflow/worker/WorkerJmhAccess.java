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

import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.NativeReaderIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Package-private bridge exposing worker internals to JMH benchmarks in {@code
 * org.apache.beam.runners.dataflow.worker.jmh} without altering production visibility.
 */
@SuppressWarnings("unchecked")
public final class WorkerJmhAccess {
  private WorkerJmhAccess() {}

  public static <InputT, OutputT, W extends BoundedWindow> ParDoFn createSimpleParDoFn(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, W> windowingStrategy,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTagsToReceiverIndices,
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext) {
    DoFnInfo<InputT, OutputT> info =
        DoFnInfo.forFn(
            fn,
            windowingStrategy,
            Collections.emptyList(),
            inputCoder,
            outputCoders,
            mainOutputTag,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    return new SimpleParDoFn<>(
        options,
        DoFnInstanceManagers.singleInstance(info),
        NullSideInputReader.empty(),
        mainOutputTag,
        outputTagsToReceiverIndices,
        executionContext.getStepContext(operationContext),
        operationContext,
        DoFnSchemaInformation.create(),
        Collections.emptyMap(),
        SimpleDoFnRunnerFactory.INSTANCE);
  }

  public static <K, V>
      NativeReaderIterator<WindowedValue<KV<K, Reiterable<V>>>> createGroupingShuffleReaderIterator(
          GroupingShuffleReader<K, V> reader, ShuffleEntryReader entryReader) {
    return reader.iterator(entryReader);
  }
}
