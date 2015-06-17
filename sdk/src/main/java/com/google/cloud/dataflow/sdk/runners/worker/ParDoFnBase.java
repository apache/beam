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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunner.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.StreamingSideInputDoFnRunner;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.OutputReceiver;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.Receiver;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A base class providing simple set up, processing, and tear down for a wrapped
 * {@link DoFn}.
 *
 * <p>Subclasses override just a method to provide a {@link DoFnInfo} for the
 * wrapped {@link DoFn}.
 */
public abstract class ParDoFnBase extends ParDoFn {

  private final PipelineOptions options;
  private final PTuple sideInputValues;
  private final TupleTag<Object> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  private final String stepName;
  private final ExecutionContext executionContext;
  private final CounterSet.AddCounterMutator addCounterMutator;

  /** The DoFnRunner executing a batch. Null between batches. */
  private DoFnRunner<Object, Object, Receiver> fnRunner;

  public ExecutionContext getExecutionContext() {
    return executionContext;
  }

  /**
   * Creates a {@link ParDoFnBase} using basic information about the step being executed.
   */
  protected ParDoFnBase(
      PipelineOptions options,
      PTuple sideInputValues,
      List<String> outputTags,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    this.options = options;

    // We vend a freshly deserialized version for each run
    this.sideInputValues = sideInputValues;
    Preconditions.checkArgument(
      outputTags.size() > 0,
      "expected at least one output");
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

  /**
   * Creates a fresh {@link DoFnInfo}. This will be called for each bundle.
   */
  protected abstract DoFnInfo<?, ?> getDoFnInfo();

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

    @SuppressWarnings("unchecked")
    DoFnInfo<Object, Object> doFnInfo = (DoFnInfo<Object, Object>) getDoFnInfo();

    OutputManager<Receiver> outputManager = new OutputManager<Receiver>() {
      final Map<TupleTag<?>, OutputReceiver> undeclaredOutputs =
      new HashMap<>();

      @Override
      public Receiver initialize(TupleTag<?> tag) {
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
    };

    if (options.as(StreamingOptions.class).isStreaming() && !sideInputValues.getAll().isEmpty()) {
      fnRunner = new StreamingSideInputDoFnRunner<Object, Object, Receiver, BoundedWindow>(
          options,
          doFnInfo,
          sideInputValues,
          outputManager,
          mainOutputTag,
          sideOutputTags,
          stepContext,
          addCounterMutator);
    } else {
      fnRunner = DoFnRunner.create(
          options,
          doFnInfo.getDoFn(),
          sideInputValues,
          outputManager,
          mainOutputTag,
          sideOutputTags,
          stepContext,
          addCounterMutator,
          doFnInfo.getWindowingStrategy());
    }

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
