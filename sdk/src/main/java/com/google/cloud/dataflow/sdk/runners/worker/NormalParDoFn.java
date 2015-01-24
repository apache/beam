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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunner.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.OutputReceiver;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.Receiver;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A wrapper around a decoded user DoFn.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NormalParDoFn extends ParDoFn {

  /**
   * Factory for creating DoFn instances.
   */
  protected static interface DoFnInfoFactory {
    public DoFnInfo createDoFnInfo() throws Exception;
  }

  public static NormalParDoFn create(
      PipelineOptions options,
      final CloudObject cloudUserFn,
      String stepName,
      @Nullable List<SideInputInfo> sideInputInfos,
      @Nullable List<MultiOutputInfo> multiOutputInfos,
      Integer numOutputs,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler /* ignored */)
      throws Exception {
    DoFnInfoFactory fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() throws Exception {
          Object deserializedFn =
              SerializableUtils.deserializeFromByteArray(
                  getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
                  "serialized user fn");
          if (!(deserializedFn instanceof DoFnInfo)) {
            throw new Exception(
                "unexpected kind of DoFnInfo: " + deserializedFn.getClass().getName());
          }
          return (DoFnInfo) deserializedFn;
        }
      };

    PTuple sideInputValues = PTuple.empty();
    if (sideInputInfos != null) {
      for (SideInputInfo sideInputInfo : sideInputInfos) {
        Object sideInputValue = SideInputUtils.readSideInput(
            options, sideInputInfo, executionContext);
        TupleTag<Object> tag = new TupleTag<>(sideInputInfo.getTag());
        sideInputValues = sideInputValues.and(tag, sideInputValue);
      }
    }

    List<String> outputTags = new ArrayList<>();
    if (multiOutputInfos != null) {
      for (MultiOutputInfo multiOutputInfo : multiOutputInfos) {
        outputTags.add(multiOutputInfo.getTag());
      }
    }
    if (outputTags.isEmpty()) {
      // Legacy support: assume there's a single output tag named "output".
      // (The output tag name will be ignored, for the main output.)
      outputTags.add("output");
    }
    if (numOutputs != outputTags.size()) {
      throw new AssertionError(
          "unexpected number of outputTags for DoFn");
    }

    return new NormalParDoFn(options, fnFactory, sideInputValues, outputTags,
                             stepName, executionContext, addCounterMutator);
  }

  public final PipelineOptions options;
  public final DoFnInfoFactory fnFactory;
  public final PTuple sideInputValues;
  public final TupleTag<Object> mainOutputTag;
  public final List<TupleTag<?>> sideOutputTags;
  public final String stepName;
  public final ExecutionContext executionContext;
  private final CounterSet.AddCounterMutator addCounterMutator;

  /** The DoFnRunner executing a batch. Null between batches. */
  DoFnRunner<Object, Object, Receiver> fnRunner;

  public NormalParDoFn(PipelineOptions options,
                       DoFnInfoFactory fnFactory,
                       PTuple sideInputValues,
                       List<String> outputTags,
                       String stepName,
                       ExecutionContext executionContext,
                       CounterSet.AddCounterMutator addCounterMutator) {
    this.options = options;
    this.fnFactory = fnFactory;
    this.sideInputValues = sideInputValues;
    if (outputTags.size() < 1) {
      throw new AssertionError("expected at least one output");
    }
    this.mainOutputTag = new TupleTag<>(outputTags.get(0));
    this.sideOutputTags = new ArrayList<>();
    if (outputTags.size() > 1) {
      for (String tag : outputTags.subList(1, outputTags.size())) {
        this.sideOutputTags.add(new TupleTag<Object>(tag));
      }
    }
    this.stepName = stepName;
    this.executionContext = executionContext;
    this.addCounterMutator = addCounterMutator;
  }

  @Override
  public void startBundle(final Receiver... receivers) throws Exception {
    if (receivers.length != sideOutputTags.size() + 1) {
      throw new AssertionError(
          "unexpected number of receivers for DoFn");
    }

    StepContext stepContext = null;
    if (executionContext != null) {
      stepContext = executionContext.getStepContext(stepName);
    }

    fnRunner = DoFnRunner.create(
        options,
        fnFactory.createDoFnInfo().getDoFn(),
        sideInputValues,
        new OutputManager<Receiver>() {
          final Map<TupleTag<?>, OutputReceiver> undeclaredOutputs =
              new HashMap<>();

          @Override
          public Receiver initialize(TupleTag tag) {
            // Declared outputs.
            if (tag.equals(mainOutputTag)) {
              return receivers[0];
            } else if (sideOutputTags.contains(tag)) {
              return receivers[sideOutputTags.indexOf(tag) + 1];
            }

            // Undeclared outputs.
            OutputReceiver receiver = undeclaredOutputs.get(tag);
            if (receiver == null) {
              // A new undeclared output.
              // TODO: plumb through the operationName, so that we can
              // name implicit outputs after it.
              String outputName = "implicit-" + tag.getId();
              // TODO: plumb through the counter prefix, so we can
              // make it available to the OutputReceiver class in case
              // it wants to use it in naming output counters.  (It
              // doesn't today.)
              String counterPrefix = "";
              receiver = new OutputReceiver(
                  outputName, counterPrefix, addCounterMutator);
              undeclaredOutputs.put(tag, receiver);
            }
            return receiver;
          }

          @Override
          public void output(Receiver receiver, WindowedValue<?> output) {
            try {
              receiver.process(output);
            } catch (Throwable t) {
              throw Throwables.propagate(t);
            }
          }
        },
        mainOutputTag,
        sideOutputTags,
        stepContext,
        addCounterMutator,
        fnFactory.createDoFnInfo().getWindowFn());

    fnRunner.startBundle();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object elem) throws Exception {
    fnRunner.processElement((WindowedValue<Object>) elem);
  }

  @Override
  public void finishBundle() throws Exception {
    fnRunner.finishBundle();
    fnRunner = null;
  }
}
