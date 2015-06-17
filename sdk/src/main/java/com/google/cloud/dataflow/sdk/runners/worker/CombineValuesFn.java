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
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.KV;
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
      Combine.KeyedCombineFn<?, ?, ?, ?> combineFn,
      String phase,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    return new CombineValuesFn(
        options, combineFn, phase, stepName, executionContext, addCounterMutator);
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
        @Nullable List<SideInputInfo> sideInputInfos,
        @Nullable List<MultiOutputInfo> multiOutputInfos,
        int numOutputs,
        ExecutionContext executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        StateSampler stateSampler)
            throws Exception {

      Preconditions.checkArgument(
          sideInputInfos == null || sideInputInfos.size() == 0,
          "unexpected side inputs for CombineValuesFn");
      Preconditions.checkArgument(
          numOutputs == 1, "expected exactly one output for CombineValuesFn");

      Object deserializedFn =
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
              "serialized user fn");
      Preconditions.checkArgument(
          deserializedFn instanceof Combine.KeyedCombineFn);
      Combine.KeyedCombineFn<?, ?, ?, ?> combineFn =
          (Combine.KeyedCombineFn<?, ?, ?, ?>) deserializedFn;

      // Get the combine phase, default to ALL. (The implementation
      // doesn't have to split the combiner).
      String phase = getString(cloudUserFn, PropertyNames.PHASE, CombinePhase.ALL);

      return CombineValuesFn.of(
          options,
          combineFn,
          phase,
          stepName,
          executionContext,
          addCounterMutator);
    }
  }

  @Override
  protected DoFnInfo<?, ?> getDoFnInfo() {
    DoFn doFn = null;
    switch (phase) {
      case CombinePhase.ALL:
        doFn = new CombineValuesDoFn(combineFn);
        break;
      case CombinePhase.ADD:
        doFn = new AddInputsDoFn(combineFn);
        break;
      case CombinePhase.MERGE:
        doFn = new MergeAccumulatorsDoFn(combineFn);
        break;
      case CombinePhase.EXTRACT:
        doFn = new ExtractOutputDoFn(combineFn);
        break;
      default:
        throw new IllegalArgumentException(
            "phase must be one of 'all', 'add', 'merge', 'extract'");
    }
    return new DoFnInfo<>(doFn, null);
  }

  private final String phase;
  private final Combine.KeyedCombineFn<?, ?, ?, ?> combineFn;

  private CombineValuesFn(
      PipelineOptions options,
      Combine.KeyedCombineFn<?, ?, ?, ?> combineFn,
      String phase,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        PTuple.empty(),
        Arrays.asList("output"),
        stepName,
        executionContext,
        addCounterMutator);
    this.phase = phase;
    this.combineFn = combineFn;
  }

  /**
   * The ALL phase is the unsplit combiner, in case combiner lifting
   * is disabled or the optimizer chose not to lift this combiner.
   */
  private static class CombineValuesDoFn<K, InputT, OutputT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>>{
    private static final long serialVersionUID = 0L;

    private final Combine.KeyedCombineFn<K, InputT, ?, OutputT> combineFn;

    private CombineValuesDoFn(
        Combine.KeyedCombineFn<K, InputT, ?, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<InputT>> kv = c.element();
      K key = kv.getKey();

      c.output(KV.of(key, this.combineFn.apply(key, kv.getValue())));
    }
  }

  /*
   * ADD phase: KV<K, Iterable<InputT>> -> KV<K, AccumT>.
   */
  private static class AddInputsDoFn<K, InputT, AccumT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, AccumT>>{
    private static final long serialVersionUID = 0L;

    private final Combine.KeyedCombineFn<K, InputT, AccumT, ?> combineFn;

    private AddInputsDoFn(
        Combine.KeyedCombineFn<K, InputT, AccumT, ?> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<InputT>> kv = c.element();
      K key = kv.getKey();
      AccumT accum = this.combineFn.createAccumulator(key);
      for (InputT input : kv.getValue()) {
        accum = this.combineFn.addInput(key, accum, input);
      }

      c.output(KV.of(key, accum));
    }
  }

  /*
   * MERGE phase: KV<K, Iterable<AccumT>> -> KV<K, AccumT>.
   */
  private static class MergeAccumulatorsDoFn<K, AccumT>
      extends DoFn<KV<K, Iterable<AccumT>>, KV<K, AccumT>>{
    private static final long serialVersionUID = 0L;

    private final Combine.KeyedCombineFn<K, ?, AccumT, ?> combineFn;

    private MergeAccumulatorsDoFn(
        Combine.KeyedCombineFn<K, ?, AccumT, ?> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<AccumT>> kv = c.element();
      K key = kv.getKey();
      AccumT accum = this.combineFn.mergeAccumulators(key, kv.getValue());

      c.output(KV.of(key, accum));
    }
  }

  /*
   * EXTRACT phase: KV<K, AccumT> -> KV<K, OutputT>.
   */
  private static class ExtractOutputDoFn<K, AccumT, OutputT>
      extends DoFn<KV<K, AccumT>, KV<K, OutputT>>{
    private static final long serialVersionUID = 0L;

    private final Combine.KeyedCombineFn<K, ?, AccumT, OutputT> combineFn;

    private ExtractOutputDoFn(
        Combine.KeyedCombineFn<K, ?, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, AccumT> kv = c.element();
      K key = kv.getKey();
      OutputT output = this.combineFn.extractOutput(key, kv.getValue());

      c.output(KV.of(key, output));
    }
  }
}
