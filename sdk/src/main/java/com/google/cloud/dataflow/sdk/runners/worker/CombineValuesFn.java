/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getBytes;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.AppliedCombineFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.PerKeyCombineFnRunner;
import com.google.cloud.dataflow.sdk.util.PerKeyCombineFnRunners;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link ParDoFn} wrapping a decoded user {@link CombineFn}.
 */
class CombineValuesFn extends ParDoFnBase {
  /**
   * The optimizer may split run the user combiner in 3 separate
   * phases (ADD, MERGE, and EXTRACT), on separate VMs, as it sees
   * fit. The CombinerPhase dictates which DoFn is actually running in
   * the worker.
   */
   // TODO: These strings are part of the service definition, and
   // should be added into the definition of the ParDoInstruction,
   // but the protiary definitions don't allow for enums yet.
  public static class CombinePhase {
    public static final String ALL = "all";
    public static final String ADD = "add";
    public static final String MERGE = "merge";
    public static final String EXTRACT = "extract";
  }

  static CombineValuesFn of(
      PipelineOptions options,
      AppliedCombineFn<?, ?, ?, ?> combineFn,
      String phase,
      SideInputReader sideInputReader,
      String stepName,
      String transformName,
      DataflowExecutionContext<?> executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
      throws Exception {
    return new CombineValuesFn(options, combineFn, phase, sideInputReader, stepName, transformName,
        executionContext, addCounterMutator, stateSampler);
  }

  /**
   * A {@link ParDoFnFactory} to create instances of {@link CombineValuesFn} according to
   * specifications from the Dataflow service.
   */
  static final class Factory implements ParDoFnFactory {
    @Override
    public ParDoFn create(
        PipelineOptions options,
        final CloudObject cloudUserFn,
        String stepName,
        String transformName,
        @Nullable List<SideInputInfo> sideInputInfos,
        @Nullable List<MultiOutputInfo> multiOutputInfos,
        int numOutputs,
        DataflowExecutionContext<?> executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        StateSampler stateSampler)
            throws Exception {

      Preconditions.checkArgument(
          numOutputs == 1, "expected exactly one output for CombineValuesFn");

      Object deserializedFn =
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
              "serialized user fn");
      Preconditions.checkArgument(deserializedFn instanceof AppliedCombineFn);
      AppliedCombineFn<?, ?, ?, ?> combineFn = (AppliedCombineFn<?, ?, ?, ?>) deserializedFn;
      Iterable<PCollectionView<?>> sideInputViews = combineFn.getSideInputViews();
      final SideInputReader sideInputReader =
          executionContext.getSideInputReader(sideInputInfos, sideInputViews);

      // Get the combine phase, default to ALL. (The implementation
      // doesn't have to split the combiner).
      String phase = getString(cloudUserFn, PropertyNames.PHASE, CombinePhase.ALL);

      return CombineValuesFn.of(
          options,
          combineFn,
          phase,
          sideInputReader,
          stepName,
          transformName,
          executionContext,
          addCounterMutator,
          stateSampler);
    }
  }

  @Override
  protected DoFnInfo<?, ?> getDoFnInfo() {
    PerKeyCombineFnRunner<?, ?, ?, ?> combineFnRunner =
        PerKeyCombineFnRunners.create(combineFn.getFn());
    DoFn<?, ?> doFn = null;
    switch (phase) {
      case CombinePhase.ALL:
        doFn = new CombineValuesDoFn<>(combineFnRunner);
        break;
      case CombinePhase.ADD:
        doFn = new AddInputsDoFn<>(combineFnRunner);
        break;
      case CombinePhase.MERGE:
        doFn = new MergeAccumulatorsDoFn<>(combineFnRunner);
        break;
      case CombinePhase.EXTRACT:
        doFn = new ExtractOutputDoFn<>(combineFnRunner);
        break;
      default:
        throw new IllegalArgumentException(
            "phase must be one of 'all', 'add', 'merge', 'extract'");
    }

    Coder inputCoder = null;
    if (combineFn.getKvCoder() != null) {
      switch (phase) {
        case CombinePhase.ALL:
          inputCoder = KvCoder.of(
              combineFn.getKvCoder().getKeyCoder(),
              IterableCoder.of(combineFn.getKvCoder().getValueCoder()));
          break;
        case CombinePhase.ADD:
          inputCoder = combineFn.getKvCoder();
          break;
        case CombinePhase.MERGE:
          inputCoder = KvCoder.of(
              combineFn.getKvCoder().getKeyCoder(),
              IterableCoder.of(combineFn.getAccumulatorCoder()));
          break;
        case CombinePhase.EXTRACT:
          inputCoder =
              KvCoder.of(combineFn.getKvCoder().getKeyCoder(), combineFn.getAccumulatorCoder());
          break;
      }
    }
    return new DoFnInfo<>(
        doFn, combineFn.getWindowingStrategy(), combineFn.getSideInputViews(), inputCoder);
  }

  private final String phase;
  private final AppliedCombineFn<?, ?, ?, ?> combineFn;

  private CombineValuesFn(
      PipelineOptions options,
      AppliedCombineFn<?, ?, ?, ?> combineFn,
      String phase,
      SideInputReader sideInputReader,
      String stepName,
      String transformName,
      DataflowExecutionContext<?> executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    super(
        options,
        sideInputReader,
        Arrays.asList("output"),
        stepName,
        transformName,
        executionContext,
        addCounterMutator,
        stateSampler);
    this.phase = phase;
    this.combineFn = combineFn;
  }

  /**
   * The ALL phase is the unsplit combiner, in case combiner lifting
   * is disabled or the optimizer chose not to lift this combiner.
   */
  private static class CombineValuesDoFn<K, InputT, OutputT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>>{
    private final PerKeyCombineFnRunner<K, InputT, ?, OutputT> combinefnRunner;

    private CombineValuesDoFn(PerKeyCombineFnRunner<K, InputT, ?, OutputT> combinefnRunner) {
      this.combinefnRunner = combinefnRunner;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<InputT>> kv = c.element();
      K key = kv.getKey();

      c.output(KV.of(key, this.combinefnRunner.apply(key, kv.getValue(), c)));
    }
  }

  /*
   * ADD phase: KV<K, Iterable<InputT>> -> KV<K, AccumT>.
   */
  private static class AddInputsDoFn<K, InputT, AccumT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, AccumT>>{
    private final PerKeyCombineFnRunner<K, InputT, AccumT, ?> combinefnRunner;

    private AddInputsDoFn(PerKeyCombineFnRunner<K, InputT, AccumT, ?> combinefnRunner) {
      this.combinefnRunner = combinefnRunner;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<InputT>> kv = c.element();
      K key = kv.getKey();
      AccumT accum = combinefnRunner.addInputs(key, kv.getValue(), c);
      c.output(KV.of(key, accum));
    }
  }

  /*
   * MERGE phase: KV<K, Iterable<AccumT>> -> KV<K, AccumT>.
   */
  private static class MergeAccumulatorsDoFn<K, AccumT>
      extends DoFn<KV<K, Iterable<AccumT>>, KV<K, AccumT>>{
    private final PerKeyCombineFnRunner<K, ?, AccumT, ?> combinefnRunner;

    private MergeAccumulatorsDoFn(PerKeyCombineFnRunner<K, ?, AccumT, ?> combinefnRunner) {
      this.combinefnRunner = combinefnRunner;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<AccumT>> kv = c.element();
      K key = kv.getKey();
      AccumT accum = this.combinefnRunner.mergeAccumulators(key, kv.getValue(), c);
      c.output(KV.of(key, accum));
    }
  }

  /*
   * EXTRACT phase: KV<K, AccumT> -> KV<K, OutputT>.
   */
  private static class ExtractOutputDoFn<K, AccumT, OutputT>
      extends DoFn<KV<K, AccumT>, KV<K, OutputT>>{
    private final PerKeyCombineFnRunner<K, ?, AccumT, OutputT> combinefnRunner;

    private ExtractOutputDoFn(PerKeyCombineFnRunner<K, ?, AccumT, OutputT> combinefnRunner) {
      this.combinefnRunner = combinefnRunner;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, AccumT> kv = c.element();
      K key = kv.getKey();
      OutputT output = this.combinefnRunner.extractOutput(key, kv.getValue(), c);
      c.output(KV.of(key, output));
    }
  }
}
