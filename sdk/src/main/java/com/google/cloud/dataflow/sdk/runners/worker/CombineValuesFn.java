/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A wrapper around a decoded user value combining function.
 */
@SuppressWarnings({"rawtypes", "serial", "unchecked"})
public class CombineValuesFn extends NormalParDoFn {
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

  public static CombineValuesFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      String stepName,
      @Nullable List<SideInputInfo> sideInputInfos,
      @Nullable List<MultiOutputInfo> multiOutputInfos,
      Integer numOutputs,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler /* unused */)
      throws Exception {
    Object deserializedFn =
        SerializableUtils.deserializeFromByteArray(
            getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
            "serialized user fn");
    Preconditions.checkArgument(
        deserializedFn instanceof Combine.KeyedCombineFn);
    final Combine.KeyedCombineFn combineFn = (Combine.KeyedCombineFn) deserializedFn;

    // Get the combine phase, default to ALL. (The implementation
    // doesn't have to split the combiner).
    final String phase = getString(cloudUserFn, PropertyNames.PHASE, CombinePhase.ALL);

    Preconditions.checkArgument(
        sideInputInfos == null || sideInputInfos.size() == 0,
        "unexpected side inputs for CombineValuesFn");
    Preconditions.checkArgument(
        numOutputs == 1, "expected exactly one output for CombineValuesFn");

    DoFnInfoFactory fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() {
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
          return new DoFnInfo(doFn, null);
        }
      };
    return new CombineValuesFn(options, fnFactory, stepName, executionContext, addCounterMutator);
  }

  private CombineValuesFn(
      PipelineOptions options,
      DoFnInfoFactory fnFactory,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        fnFactory,
        PTuple.empty(),
        Arrays.asList("output"),
        stepName,
        executionContext,
        addCounterMutator);
  }

  /**
   * The ALL phase is the unsplit combiner, in case combiner lifting
   * is disabled or the optimizer chose not to lift this combiner.
   */
  private static class CombineValuesDoFn<K, VI, VO>
      extends DoFn<KV<K, Iterable<VI>>, KV<K, VO>>{
    private final Combine.KeyedCombineFn<K, VI, ?, VO> combineFn;

    private CombineValuesDoFn(
        Combine.KeyedCombineFn<K, VI, ?, VO> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<VI>> kv = c.element();
      K key = kv.getKey();

      c.output(KV.of(key, this.combineFn.apply(key, kv.getValue())));
    }
  }

  /*
   * ADD phase: KV<K, Iterable<VI>> -> KV<K, VA>.
   */
  private static class AddInputsDoFn<K, VI, VA>
      extends DoFn<KV<K, Iterable<VI>>, KV<K, VA>>{
    private final Combine.KeyedCombineFn<K, VI, VA, ?> combineFn;

    private AddInputsDoFn(
        Combine.KeyedCombineFn<K, VI, VA, ?> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<VI>> kv = c.element();
      K key = kv.getKey();
      VA accum = this.combineFn.createAccumulator(key);
      for (VI input : kv.getValue()) {
        this.combineFn.addInput(key, accum, input);
      }

      c.output(KV.of(key, accum));
    }
  }

  /*
   * MERGE phase: KV<K, Iterable<VA>> -> KV<K, VA>.
   */
  private static class MergeAccumulatorsDoFn<K, VA>
      extends DoFn<KV<K, Iterable<VA>>, KV<K, VA>>{
    private final Combine.KeyedCombineFn<K, ?, VA, ?> combineFn;

    private MergeAccumulatorsDoFn(
        Combine.KeyedCombineFn<K, ?, VA, ?> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, Iterable<VA>> kv = c.element();
      K key = kv.getKey();
      VA accum = this.combineFn.mergeAccumulators(key, kv.getValue());

      c.output(KV.of(key, accum));
    }
  }

  /*
   * EXTRACT phase: KV<K, Iterable<VA>> -> KV<K, VA>.
   */
  private static class ExtractOutputDoFn<K, VA, VO>
      extends DoFn<KV<K, VA>, KV<K, VO>>{
    private final Combine.KeyedCombineFn<K, ?, VA, VO> combineFn;

    private ExtractOutputDoFn(
        Combine.KeyedCombineFn<K, ?, VA, VO> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<K, VA> kv = c.element();
      K key = kv.getKey();
      VO output = this.combineFn.extractOutput(key, kv.getValue());

      c.output(KV.of(key, output));
    }
  }
}
