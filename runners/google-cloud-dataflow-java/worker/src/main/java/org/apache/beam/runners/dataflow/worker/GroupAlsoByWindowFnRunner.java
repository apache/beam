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

import java.util.Collection;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Runs a {@link GroupAlsoByWindowFn} by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the {@link GroupAlsoByWindowFn} (main) input elements
 * @param <OutputT> the type of the {@link GroupAlsoByWindowFn} (main) output elements
 */
public class GroupAlsoByWindowFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private final PipelineOptions options;
  /** The {@link GroupAlsoByWindowFn} being run. */
  private final GroupAlsoByWindowFn<InputT, OutputT> fn;

  private final SideInputReader sideInputReader;
  private final OutputManager outputManager;
  private final TupleTag<OutputT> mainOutputTag;
  private final StepContext stepContext;

  public GroupAlsoByWindowFnRunner(
      PipelineOptions options,
      GroupAlsoByWindowFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      StepContext stepContext) {
    this.options = options;
    this.fn = fn;
    this.sideInputReader = sideInputReader;
    this.outputManager = outputManager;
    this.mainOutputTag = mainOutputTag;
    this.stepContext = stepContext;
  }

  @Override
  public void startBundle() {}

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    if (elem.getWindows().size() <= 1 || sideInputReader.isEmpty()) {
      invokeProcessElement(elem);
    } else {
      // We could modify the windowed value (and the processContext) to
      // avoid repeated allocations, but this is more straightforward.
      for (WindowedValue<InputT> windowedValue : elem.explodeWindows()) {
        invokeProcessElement(windowedValue);
      }
    }
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException(
        String.format("Timers are not supported by %s", GroupAlsoByWindowFn.class.getSimpleName()));
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      OutputWindowedValue<OutputT> output =
          new OutputWindowedValue<OutputT>() {
            @Override
            public void outputWindowedValue(
                OutputT output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo pane) {
              WindowedValue<OutputT> windowed = WindowedValue.of(output, timestamp, windows, pane);
              outputManager.output(mainOutputTag, windowed);
            }

            @Override
            public <AdditionalOutputT> void outputWindowedValue(
                TupleTag<AdditionalOutputT> tag,
                AdditionalOutputT output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo pane) {
              throw new UnsupportedOperationException();
            }
          };
      fn.processElement(elem.getValue(), options, stepContext, sideInputReader, output);
    } catch (Exception ex) {
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }

      throw new RuntimeException(ex);
    }
  }

  @Override
  public void finishBundle() {}

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

  @Override
  public DoFn<InputT, OutputT> getFn() {
    throw new UnsupportedOperationException(
        String.format("%s does not support getFn()", getClass().getCanonicalName()));
  }
}
