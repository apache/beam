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

package org.apache.beam.runners.gearpump.translators.functions;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.DoFnRunnerFactory;
import org.apache.beam.runners.gearpump.translators.utils.NoOpStepContext;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils.RawUnionValue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.gearpump.streaming.dsl.javaapi.functions.FlatMapFunction;

/**
 * Gearpump {@link FlatMapFunction} wrapper over Beam {@link DoFn}.
 */
@SuppressWarnings("unchecked")
public class DoFnFunction<InputT, OutputT> extends
    FlatMapFunction<List<RawUnionValue>, RawUnionValue> {

  private static final long serialVersionUID = -5701440128544343353L;
  private final DoFnRunnerFactory<InputT, OutputT> doFnRunnerFactory;
  private final DoFn<InputT, OutputT> doFn;
  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
  private transient PushbackSideInputDoFnRunner<InputT, OutputT> doFnRunner;
  private transient SideInputHandler sideInputReader;
  private transient List<WindowedValue<InputT>> pushedBackValues;
  private final Collection<PCollectionView<?>> sideInputs;
  private final Map<String, PCollectionView<?>> tagsToSideInputs;
  private final TupleTag<OutputT> mainOutput;
  private final List<TupleTag<?>> sideOutputs;
  private final DoFnOutputManager outputManager;

  public DoFnFunction(
      GearpumpPipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Collection<PCollectionView<?>> sideInputs,
      Map<String, PCollectionView<?>> sideInputTagMapping,
      TupleTag<OutputT> mainOutput,
      List<TupleTag<?>> sideOutputs) {
    this.doFn = doFn;
    this.outputManager = new DoFnOutputManager();
    this.doFnRunnerFactory = new DoFnRunnerFactory<>(
        pipelineOptions,
        doFn,
        sideInputs,
        outputManager,
        mainOutput,
        sideOutputs,
        new NoOpStepContext(),
        windowingStrategy
    );
    this.sideInputs = sideInputs;
    this.tagsToSideInputs = sideInputTagMapping;
    this.mainOutput = mainOutput;
    this.sideOutputs = sideOutputs;
  }

  @Override
  public void setup() {
    sideInputReader = new SideInputHandler(sideInputs,
        InMemoryStateInternals.<Void>forKey(null));
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    doFnRunner = doFnRunnerFactory.createRunner(sideInputReader);

    pushedBackValues = new LinkedList<>();
    outputManager.setup(mainOutput, sideOutputs);
  }

  @Override
  public void teardown() {
    doFnInvoker.invokeTeardown();
  }

  @Override
  public Iterator<TranslatorUtils.RawUnionValue> flatMap(List<RawUnionValue> inputs) {
    outputManager.clear();

    doFnRunner.startBundle();

    for (RawUnionValue unionValue: inputs) {
      final String tag = unionValue.getUnionTag();
      if ("0".equals(tag)) {
        // main input
        pushedBackValues.add((WindowedValue<InputT>) unionValue.getValue());
      } else {
        // side input
        PCollectionView<?> sideInput = tagsToSideInputs.get(unionValue.getUnionTag());
        WindowedValue<Iterable<?>> sideInputValue =
            (WindowedValue<Iterable<?>>) unionValue.getValue();
        sideInputReader.addSideInputValue(sideInput, sideInputValue);
      }
    }

    for (PCollectionView<?> sideInput: sideInputs) {
      for (WindowedValue<InputT> value : pushedBackValues) {
        for (BoundedWindow win: value.getWindows()) {
          BoundedWindow sideInputWindow =
              sideInput.getWindowMappingFn().getSideInputWindow(win);
          if (!sideInputReader.isReady(sideInput, sideInputWindow)) {
            Object emptyValue = WindowedValue.of(
                Lists.newArrayList(), value.getTimestamp(), sideInputWindow, value.getPane());
            sideInputReader.addSideInputValue(sideInput, (WindowedValue<Iterable<?>>) emptyValue);
          }
        }
      }
    }

    List<WindowedValue<InputT>> nextPushedBackValues = new LinkedList<>();
    for (WindowedValue<InputT> value : pushedBackValues) {
      Iterable<WindowedValue<InputT>> values = doFnRunner.processElementInReadyWindows(value);
      Iterables.addAll(nextPushedBackValues, values);
    }
    pushedBackValues.clear();
    Iterables.addAll(pushedBackValues, nextPushedBackValues);

    doFnRunner.finishBundle();

    return outputManager.getOutputs();
  }

  private static class DoFnOutputManager implements DoFnRunners.OutputManager, Serializable {

    private static final long serialVersionUID = 4967375172737408160L;
    private transient List<RawUnionValue> outputs;
    private transient Set<TupleTag<?>> outputTags;

    @Override
    public <T> void output(TupleTag<T> outputTag, WindowedValue<T> output) {
      if (outputTags.contains(outputTag)) {
        outputs.add(new RawUnionValue(outputTag.getId(), output));
      }
    }

    void setup(TupleTag<?> mainOutput, List<TupleTag<?>> sideOutputs) {
      outputs = new LinkedList<>();
      outputTags = new HashSet<>();
      outputTags.add(mainOutput);
      outputTags.addAll(sideOutputs);
    }

    void clear() {
      outputs.clear();
    }

    Iterator<RawUnionValue> getOutputs() {
      return outputs.iterator();
    }
  }
}
