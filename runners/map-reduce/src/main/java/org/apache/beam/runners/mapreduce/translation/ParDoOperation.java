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

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation for ParDo.
 */
public class ParDoOperation implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ParDoOperation.class);

  private final DoFn<Object, Object> doFn;
  private final SerializedPipelineOptions options;
  private final TupleTag<Object> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final OutputReceiver[] receivers;

  private DoFnRunner<Object, Object> fnRunner;

  public ParDoOperation(
      DoFn<Object, Object> doFn,
      PipelineOptions options,
      TupleTag<Object> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.doFn = checkNotNull(doFn, "doFn");
    this.options = new SerializedPipelineOptions(checkNotNull(options, "options"));
    this.mainOutputTag = checkNotNull(mainOutputTag, "mainOutputTag");
    this.sideOutputTags = checkNotNull(sideOutputTags, "sideOutputTags");
    this.windowingStrategy = checkNotNull(windowingStrategy, "windowingStrategy");
    int numOutputs = 1 + sideOutputTags.size();
    this.receivers = new OutputReceiver[numOutputs];
    for (int i = 0; i < numOutputs; ++i) {
      receivers[i] = new OutputReceiver();
    }
  }

  /**
   * Adds an input to this ParDoOperation, coming from the given output of the given source.
   */
  public void attachInput(ParDoOperation source, int outputNum) {
    OutputReceiver fanOut = source.receivers[outputNum];
    fanOut.addOutput(this);
  }

  /**
   * Starts this Operation's execution.
   *
   * <p>Called after all successors consuming operations have been started.
   */
  public void start() {
    fnRunner = DoFnRunners.simpleRunner(
        options.getPipelineOptions(),
        doFn,
        NullSideInputReader.empty(),
        new ParDoOutputManager(),
        mainOutputTag,
        sideOutputTags,
        null,
        windowingStrategy);
    fnRunner.startBundle();
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (ParDoOperation parDo : receiver.getReceiverParDos()) {
        parDo.start();
      }
    }
  }

  /**
   * Processes the element.
   */
  public void process(Object elem) {
    LOG.info("elem: {}.", elem);
    fnRunner.processElement((WindowedValue<Object>) elem);
  }

  /**
   * Finishes this Operation's execution.
   *
   * <p>Called after all predecessors producing operations have been finished.
   */
  public void finish() {
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (ParDoOperation parDo : receiver.getReceiverParDos()) {
        parDo.finish();
      }
    }
    fnRunner.finishBundle();
  }

  private class ParDoOutputManager implements DoFnRunners.OutputManager {

    @Nullable
    private OutputReceiver getReceiverOrNull(TupleTag<?> tag) {
      if (tag.equals(mainOutputTag)) {
        return receivers[0];
      } else if (sideOutputTags.contains(tag)) {
        return receivers[sideOutputTags.indexOf(tag) + 1];
      } else {
        return null;
      }
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
