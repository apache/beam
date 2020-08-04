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

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.runners.dataflow.util.Structs.getString;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link ParDoFnFactory} to create instances of user {@link CombineFn} according to
 * specifications from the Dataflow service.
 */
class CombineValuesFnFactory implements ParDoFnFactory {

  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      @Nullable List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    Preconditions.checkArgument(
        outputTupleTagsToReceiverIndices.size() == 1,
        "expected exactly one output for CombineValuesFn");

    Object deserializedFn =
        SerializableUtils.deserializeFromByteArray(
            getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN), "serialized user fn");
    Preconditions.checkArgument(deserializedFn instanceof AppliedCombineFn);
    AppliedCombineFn<?, ?, ?, ?> combineFn = (AppliedCombineFn<?, ?, ?, ?>) deserializedFn;
    Iterable<PCollectionView<?>> sideInputViews = combineFn.getSideInputViews();
    SideInputReader sideInputReader =
        executionContext.getSideInputReader(sideInputInfos, sideInputViews, operationContext);

    // Get the combine phase, default to ALL. (The implementation
    // doesn't have to split the combiner).
    String phase = getString(cloudUserFn, WorkerPropertyNames.PHASE, CombinePhase.ALL);

    DoFnInfo<?, ?> doFnInfo = getDoFnInfo(combineFn, sideInputReader, phase);
    return new SimpleParDoFn(
        options,
        DoFnInstanceManagers.singleInstance(doFnInfo),
        sideInputReader,
        mainOutputTag,
        outputTupleTagsToReceiverIndices,
        executionContext.getStepContext(operationContext),
        operationContext,
        doFnInfo.getDoFnSchemaInformation(),
        doFnInfo.getSideInputMapping(),
        SimpleDoFnRunnerFactory.INSTANCE);
  }

  private static <K, InputT, AccumT, OutputT> DoFnInfo<?, ?> getDoFnInfo(
      AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn,
      SideInputReader sideInputReader,
      String phase) {
    switch (phase) {
      case CombinePhase.ALL:
        return CombineValuesDoFn.createDoFnInfo(combineFn, sideInputReader);
      case CombinePhase.ADD:
        return AddInputsDoFn.createDoFnInfo(combineFn, sideInputReader);
      case CombinePhase.MERGE:
        return MergeAccumulatorsDoFn.createDoFnInfo(combineFn, sideInputReader);
      case CombinePhase.EXTRACT:
        return ExtractOutputDoFn.createDoFnInfo(combineFn, sideInputReader);
      default:
        throw new IllegalArgumentException("phase must be one of 'all', 'add', 'merge', 'extract'");
    }
  }

  /**
   * The ALL phase is the unsplit combiner, in case combiner lifting is disabled or the optimizer
   * chose not to lift this combiner.
   */
  private static class CombineValuesDoFn<K, InputT, OutputT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>> {

    private static <K, InputT, AccumT, OutputT> DoFnInfo<?, ?> createDoFnInfo(
        AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn, SideInputReader sideInputReader) {
      GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner =
          GlobalCombineFnRunners.create(combineFn.getFn());
      DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>> doFn =
          new CombineValuesDoFn<>(combineFnRunner, sideInputReader);

      Coder<KV<K, Iterable<InputT>>> inputCoder = null;
      if (combineFn.getKvCoder() != null) {
        inputCoder =
            KvCoder.of(
                combineFn.getKvCoder().getKeyCoder(),
                IterableCoder.of(combineFn.getKvCoder().getValueCoder()));
      }
      return DoFnInfo.forFn(
          doFn,
          combineFn.getWindowingStrategy(),
          combineFn.getSideInputViews(),
          inputCoder,
          Collections.emptyMap(), // Not needed here.
          new TupleTag<>(PropertyNames.OUTPUT),
          DoFnSchemaInformation.create(),
          Collections.emptyMap());
    }

    private final GlobalCombineFnRunner<InputT, ?, OutputT> combineFnRunner;
    private final SideInputReader sideInputReader;

    private CombineValuesDoFn(
        GlobalCombineFnRunner<InputT, ?, OutputT> combineFnRunner,
        SideInputReader sideInputReader) {
      this.combineFnRunner = combineFnRunner;
      this.sideInputReader = sideInputReader;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<K, Iterable<InputT>> kv = c.element();
      c.output(
          KV.of(
              kv.getKey(),
              applyCombineFn(combineFnRunner, kv.getValue(), window, c.getPipelineOptions())));
    }

    private <AccumT> OutputT applyCombineFn(
        GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner,
        Iterable<InputT> inputs,
        BoundedWindow window,
        PipelineOptions options) {
      List<BoundedWindow> windows = Collections.singletonList(window);
      AccumT accum = combineFnRunner.createAccumulator(options, sideInputReader, windows);
      for (InputT input : inputs) {
        accum = combineFnRunner.addInput(accum, input, options, sideInputReader, windows);
      }
      return combineFnRunner.extractOutput(accum, options, sideInputReader, windows);
    }
  }

  /*
   * ADD phase: KV<K, Iterable<InputT>> -> KV<K, AccumT>.
   */
  private static class AddInputsDoFn<K, InputT, AccumT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, AccumT>> {

    private final SideInputReader sideInputReader;

    private static <K, InputT, AccumT, OutputT> DoFnInfo<?, ?> createDoFnInfo(
        AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn, SideInputReader sideInputReader) {
      GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner =
          GlobalCombineFnRunners.create(combineFn.getFn());
      DoFn<KV<K, Iterable<InputT>>, KV<K, AccumT>> doFn =
          new AddInputsDoFn<>(combineFnRunner, sideInputReader);

      Coder<KV<K, Iterable<InputT>>> inputCoder = null;
      if (combineFn.getKvCoder() != null) {
        inputCoder =
            KvCoder.of(
                combineFn.getKvCoder().getKeyCoder(),
                IterableCoder.of(combineFn.getKvCoder().getValueCoder()));
      }
      return DoFnInfo.forFn(
          doFn,
          combineFn.getWindowingStrategy(),
          combineFn.getSideInputViews(),
          inputCoder,
          Collections.emptyMap(), // Not needed here.
          new TupleTag<>(PropertyNames.OUTPUT),
          DoFnSchemaInformation.create(),
          Collections.emptyMap());
    }

    private final GlobalCombineFnRunner<InputT, AccumT, ?> combineFnRunner;

    private AddInputsDoFn(
        GlobalCombineFnRunner<InputT, AccumT, ?> combineFnRunner, SideInputReader sideInputReader) {
      this.combineFnRunner = combineFnRunner;
      this.sideInputReader = sideInputReader;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<K, Iterable<InputT>> kv = c.element();
      K key = kv.getKey();

      List<BoundedWindow> windows = Collections.singletonList(window);
      AccumT accum =
          combineFnRunner.createAccumulator(c.getPipelineOptions(), sideInputReader, windows);

      for (InputT input : kv.getValue()) {
        accum =
            combineFnRunner.addInput(
                accum, input, c.getPipelineOptions(), sideInputReader, windows);
      }

      c.output(KV.of(key, accum));
    }
  }

  /*
   * MERGE phase: KV<K, Iterable<AccumT>> -> KV<K, AccumT>.
   */
  private static class MergeAccumulatorsDoFn<K, AccumT>
      extends DoFn<KV<K, Iterable<AccumT>>, KV<K, AccumT>> {

    private final SideInputReader sideInputReader;

    private static <K, InputT, AccumT, OutputT> DoFnInfo<?, ?> createDoFnInfo(
        AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn, SideInputReader sideInputReader) {
      GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner =
          GlobalCombineFnRunners.create(combineFn.getFn());
      DoFn<KV<K, Iterable<AccumT>>, KV<K, AccumT>> doFn =
          new MergeAccumulatorsDoFn<>(combineFnRunner, sideInputReader);

      KvCoder<K, Iterable<AccumT>> inputCoder = null;
      if (combineFn.getKvCoder() != null) {
        inputCoder =
            KvCoder.of(
                combineFn.getKvCoder().getKeyCoder(),
                IterableCoder.of(combineFn.getAccumulatorCoder()));
      }
      return DoFnInfo.forFn(
          doFn,
          combineFn.getWindowingStrategy(),
          combineFn.getSideInputViews(),
          inputCoder,
          Collections.emptyMap(), // Not needed here.
          new TupleTag<>(PropertyNames.OUTPUT),
          DoFnSchemaInformation.create(),
          Collections.emptyMap());
    }

    private final GlobalCombineFnRunner<?, AccumT, ?> combineFnRunner;

    private MergeAccumulatorsDoFn(
        GlobalCombineFnRunner<?, AccumT, ?> combineFnRunner, SideInputReader sideInputReader) {
      this.combineFnRunner = combineFnRunner;
      this.sideInputReader = sideInputReader;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<K, Iterable<AccumT>> kv = c.element();
      K key = kv.getKey();
      AccumT accum =
          this.combineFnRunner.mergeAccumulators(
              kv.getValue(),
              c.getPipelineOptions(),
              sideInputReader,
              Collections.singletonList(window));
      c.output(KV.of(key, accum));
    }
  }

  /*
   * EXTRACT phase: KV<K, AccumT> -> KV<K, OutputT>.
   */
  private static class ExtractOutputDoFn<K, AccumT, OutputT>
      extends DoFn<KV<K, AccumT>, KV<K, OutputT>> {
    private static <K, InputT, AccumT, OutputT> DoFnInfo<?, ?> createDoFnInfo(
        AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn, SideInputReader sideInputReader) {
      GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFnRunner =
          GlobalCombineFnRunners.create(combineFn.getFn());
      DoFn<KV<K, AccumT>, KV<K, OutputT>> doFn =
          new ExtractOutputDoFn<>(combineFnRunner, sideInputReader);

      KvCoder<K, AccumT> inputCoder = null;
      if (combineFn.getKvCoder() != null) {
        inputCoder =
            KvCoder.of(combineFn.getKvCoder().getKeyCoder(), combineFn.getAccumulatorCoder());
      }
      return DoFnInfo.forFn(
          doFn,
          combineFn.getWindowingStrategy(),
          combineFn.getSideInputViews(),
          inputCoder,
          Collections.emptyMap(), // Not needed here.
          new TupleTag<>(PropertyNames.OUTPUT),
          DoFnSchemaInformation.create(),
          Collections.emptyMap());
    }

    private final GlobalCombineFnRunner<?, AccumT, OutputT> combineFnRunner;
    private final SideInputReader sideInputReader;

    private ExtractOutputDoFn(
        GlobalCombineFnRunner<?, AccumT, OutputT> combineFnRunner,
        SideInputReader sideInputReader) {
      this.combineFnRunner = combineFnRunner;
      this.sideInputReader = sideInputReader;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      KV<K, AccumT> kv = c.element();
      K key = kv.getKey();
      OutputT output =
          this.combineFnRunner.extractOutput(
              kv.getValue(),
              c.getPipelineOptions(),
              sideInputReader,
              Collections.singletonList(window));
      c.output(KV.of(key, output));
    }
  }
}
