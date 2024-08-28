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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.runners.flink.translation.functions.AbstractFlinkCombineRunner;
import org.apache.beam.runners.flink.translation.functions.HashingFlinkCombineRunner;
import org.apache.beam.runners.flink.translation.functions.SortingFlinkCombineRunner;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PartialReduceBundleOperator<K, InputT, OutputT, AccumT>
    extends DoFnOperator<KV<K, InputT>, KV<K, InputT>, KV<K, AccumT>> {

  private final CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn;

  private Multimap<K, WindowedValue<KV<K, InputT>>> state;
  private transient @Nullable ListState<WindowedValue<KV<K, InputT>>> checkpointedState;

  public PartialReduceBundleOperator(
      CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn,
      String stepName,
      Coder<WindowedValue<KV<K, InputT>>> windowedInputCoder,
      TupleTag<KV<K, AccumT>> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<KV<K, AccumT>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options) {
    super(
        null,
        stepName,
        windowedInputCoder,
        Collections.emptyMap(),
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        null,
        null,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());

    this.combineFn = combineFn;
    this.state = ArrayListMultimap.create();
    this.checkpointedState = null;
  }

  @Override
  public void open() throws Exception {
    clearState();
    setBundleFinishedCallback(this::finishBundle);
    super.open();
  }

  @Override
  protected boolean shoudBundleElements() {
    return true;
  }

  private void finishBundle() {
    AbstractFlinkCombineRunner<K, InputT, AccumT, AccumT, BoundedWindow> reduceRunner;
    try {
      if (windowingStrategy.needsMerge() && windowingStrategy.getWindowFn() instanceof Sessions) {
        reduceRunner = new SortingFlinkCombineRunner<>();
      } else {
        reduceRunner = new HashingFlinkCombineRunner<>();
      }

      for (Map.Entry<K, Collection<WindowedValue<KV<K, InputT>>>> e : state.asMap().entrySet()) {
        //noinspection unchecked
        reduceRunner.combine(
            new AbstractFlinkCombineRunner.PartialFlinkCombiner<>(combineFn),
            (WindowingStrategy<Object, BoundedWindow>) windowingStrategy,
            sideInputReader,
            serializedOptions.get(),
            e.getValue(),
            new Collector<WindowedValue<KV<K, AccumT>>>() {
              @Override
              public void collect(WindowedValue<KV<K, AccumT>> record) {
                outputManager.output(mainOutputTag, record);
              }

              @Override
              public void close() {}
            });
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    clearState();
  }

  private void clearState() {
    this.state = ArrayListMultimap.create();
    if (this.checkpointedState != null) {
      this.checkpointedState.clear();
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    ListStateDescriptor<WindowedValue<KV<K, InputT>>> descriptor =
        new ListStateDescriptor<>(
            "buffered-elements", new CoderTypeSerializer<>(windowedInputCoder, serializedOptions));

    checkpointedState = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored() && this.checkpointedState != null) {
      for (WindowedValue<KV<K, InputT>> wkv : this.checkpointedState.get()) {
        this.state.put(wkv.getValue().getKey(), wkv);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    if (this.checkpointedState != null) {
      this.checkpointedState.update(new ArrayList<>(this.state.values()));
    }
  }

  @Override
  protected DoFn<KV<K, InputT>, KV<K, AccumT>> getDoFn() {
    return new DoFn<KV<K, InputT>, KV<K, AccumT>>() {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        WindowedValue<KV<K, InputT>> windowedValue =
            WindowedValue.of(c.element(), c.timestamp(), window, c.pane());
        state.put(Objects.requireNonNull(c.element()).getKey(), windowedValue);
      }
    };
  }
}
