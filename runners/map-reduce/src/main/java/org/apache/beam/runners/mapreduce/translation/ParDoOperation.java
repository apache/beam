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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Operation for ParDo.
 */
public abstract class ParDoOperation<InputT, OutputT> extends Operation<InputT> {
  protected final SerializedPipelineOptions options;
  protected final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected DoFnInvoker<InputT, OutputT> doFnInvoker;
  private DoFnRunner<InputT, OutputT> fnRunner;

  public ParDoOperation(
      PipelineOptions options,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(1 + sideOutputTags.size());
    this.options = new SerializedPipelineOptions(checkNotNull(options, "options"));
    this.mainOutputTag = checkNotNull(mainOutputTag, "mainOutputTag");
    this.sideOutputTags = checkNotNull(sideOutputTags, "sideOutputTags");
    this.windowingStrategy = checkNotNull(windowingStrategy, "windowingStrategy");
  }

  /**
   * Returns a {@link DoFn} for processing inputs.
   */
  abstract DoFn<InputT, OutputT> getDoFn();

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    super.start(taskContext);
    DoFn<InputT, OutputT> doFn = getDoFn();
    // Process user's setup
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    fnRunner = DoFnRunners.simpleRunner(
        options.getPipelineOptions(),
        getDoFn(),
        NullSideInputReader.empty(),
        createOutputManager(),
        mainOutputTag,
        sideOutputTags,
        null,
        windowingStrategy);
    fnRunner.startBundle();
  }

  /**
   * Processes the element.
   */
  @Override
  public void process(WindowedValue<InputT> elem) {
    fnRunner.processElement(elem);
  }

  @Override
  public void finish() {
    fnRunner.finishBundle();
    doFnInvoker.invokeTeardown();
    super.finish();
  }

  @Override
  protected int getOutputIndex(TupleTag<?> tupleTag) {
    if (tupleTag == mainOutputTag) {
      return 0;
    } else {
      int sideIndex = sideOutputTags.indexOf(tupleTag);
      checkState(
          sideIndex >= 0,
          String.format("Cannot find index for tuple tag: %s.", tupleTag));
      return sideIndex + 1;
    }
  }

  protected DoFnRunners.OutputManager createOutputManager() {
    return new ParDoOutputManager();
  }

  private class ParDoOutputManager implements DoFnRunners.OutputManager {

    @Nullable
    private OutputReceiver getReceiverOrNull(TupleTag<?> tupleTag) {
      List<OutputReceiver> receivers = getOutputReceivers();
      int outputIndex = getOutputIndex(tupleTag);
      return receivers.get(outputIndex);
    }

    @Override
    public <T> void output(TupleTag<T> tupleTag, WindowedValue<T> windowedValue) {
      OutputReceiver receiver = getReceiverOrNull(tupleTag);
      if (receiver != null) {
        receiver.process(windowedValue);
      }
    }
  }
}
