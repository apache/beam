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

import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
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
  private final String stepName;
  protected final SerializedPipelineOptions options;
  protected final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  protected final WindowingStrategy<?, ?> windowingStrategy;
  private final List<Graphs.Tag> sideInputTags;
  private Map<TupleTag<?>, String> tupleTagToFilePath;

  private MetricsReporter metricsReporter;
  protected DoFnInvoker<InputT, OutputT> doFnInvoker;
  private DoFnRunner<InputT, OutputT> fnRunner;

  public ParDoOperation(
      String stepName,
      PipelineOptions options,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      List<Graphs.Tag> sideInputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(1 + sideOutputTags.size());
    this.stepName = checkNotNull(stepName, "stepName");
    this.options = new SerializedPipelineOptions(checkNotNull(options, "options"));
    this.mainOutputTag = checkNotNull(mainOutputTag, "mainOutputTag");
    this.sideOutputTags = checkNotNull(sideOutputTags, "sideOutputTags");
    this.windowingStrategy = checkNotNull(windowingStrategy, "windowingStrategy");
    this.sideInputTags = checkNotNull(sideInputTags, "sideInputTags");
  }

  /**
   * Returns a {@link DoFn} for processing inputs.
   */
  abstract DoFn<InputT, OutputT> getDoFn();

  @Override
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    super.start(taskContext);
    this.metricsReporter = new MetricsReporter(taskContext);

    DoFn<InputT, OutputT> doFn = getDoFn();
    // Process user's setup
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    Map<TupleTag<?>, Coder<?>> tupleTagToCoder = Maps.newHashMap();
    for (Graphs.Tag tag : sideInputTags) {
      tupleTagToCoder.put(tag.getTupleTag(), tag.getCoder());
    }

    final StateInternals stateInternals;
    try {
      stateInternals = InMemoryStateInternals.forKey(taskContext.getCurrentKey());
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(e);
    }
    final TimerInternals timerInternals = new InMemoryTimerInternals();

    fnRunner = DoFnRunners.simpleRunner(
        options.getPipelineOptions(),
        getDoFn(),
        sideInputTags.isEmpty()
            ? NullSideInputReader.empty() :
            new FileSideInputReader(tupleTagToFilePath, tupleTagToCoder, getConf().getConf()),
        createOutputManager(),
        mainOutputTag,
        sideOutputTags,
        new StepContext() {
          @Override
          public StateInternals stateInternals() {
            return stateInternals;
          }

          @Override
          public TimerInternals timerInternals() {
            return timerInternals;
          }
        },
        windowingStrategy);

    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      fnRunner.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Processes the element.
   */
  @Override
  public void process(WindowedValue<InputT> elem) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      fnRunner.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finish() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
        metricsReporter.getMetricsContainer(stepName))) {
      fnRunner.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    metricsReporter.updateMetrics();
    doFnInvoker.invokeTeardown();
    super.finish();
  }

  public void setupSideInput(Map<TupleTag<?>, String> tupleTagToFilePath) {
    this.tupleTagToFilePath = checkNotNull(tupleTagToFilePath, "tupleTagToFilePath");
  }

  public List<Graphs.Tag> getSideInputTags() {
    return sideInputTags;
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
