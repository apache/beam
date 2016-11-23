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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.DoFnRunnerFactory;
import org.apache.beam.runners.gearpump.translators.utils.NoOpAggregatorFactory;
import org.apache.beam.runners.gearpump.translators.utils.NoOpStepContext;
import org.apache.beam.runners.gearpump.translators.utils.RawUnionValue;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;

import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;

/**
 * Gearpump {@link FlatMapFunction} wrapper over Beam {@link DoFn}.
 */
@SuppressWarnings("unchecked")
public class DoFnFunction<InputT, OutputT> implements
    FlatMapFunction<RawUnionValue, RawUnionValue> {

  private final DoFnRunnerFactory<InputT, OutputT> doFnRunnerFactory;
  private PushbackSideInputDoFnRunner<InputT, OutputT> doFnRunner;
  private SideInputHandler sideInputReader;
  private List<WindowedValue<InputT>> pushedBackValues;
  private final Collection<PCollectionView<?>> sideInputs;
  private final Map<Integer, PCollectionView<?>> tagsToSideInputs;
  private final DoFnOutputManager outputManager;

  public DoFnFunction(
      GearpumpPipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Collection<PCollectionView<?>> sideInputs,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      final TupleTag<OutputT> mainOutput,
      final List<TupleTag<?>> sideOutputs,
      final Map<TupleTag<?>, Integer> sideOutputsToTags) {
    this.outputManager = new DoFnOutputManager(mainOutput, sideOutputsToTags);
    this.doFnRunnerFactory = new DoFnRunnerFactory<>(
        pipelineOptions,
        doFn,
        sideInputs,
        outputManager,
        mainOutput,
        sideOutputs,
        new NoOpStepContext(),
        new NoOpAggregatorFactory(),
        windowingStrategy
    );
    this.sideInputs = sideInputs;
    this.tagsToSideInputs = sideInputTagMapping;
  }

  @Override
  public Iterator<RawUnionValue> apply(RawUnionValue unionValue) {
    outputManager.clear();

    if (null == sideInputReader) {
      sideInputReader = new SideInputHandler(sideInputs,
        InMemoryStateInternals.<Void>forKey(null));
    }

    if (null == doFnRunner) {
      doFnRunner = doFnRunnerFactory.createRunner(sideInputReader);
    }

    if (null == pushedBackValues) {
      pushedBackValues = new LinkedList<>();
    }

    doFnRunner.startBundle();

    final int tag = unionValue.getUnionTag();
    if (tag == 0) {
      // main input
      Iterable<WindowedValue<InputT>> values =
          doFnRunner.processElementInReadyWindows((WindowedValue<InputT>) unionValue.getValue());
      Iterables.addAll(pushedBackValues, values);
    } else {
      // side input
      PCollectionView<?> sideInput = tagsToSideInputs.get(unionValue.getUnionTag());
      sideInputReader.addSideInputValue(sideInput,
          (WindowedValue<Iterable<?>>) unionValue.getValue());

      List<WindowedValue<InputT>> nextPushedBackValules = new LinkedList<>();
      for (WindowedValue<InputT> value: pushedBackValues) {
        Iterable<WindowedValue<InputT>> values = doFnRunner.processElementInReadyWindows(value);
        Iterables.addAll(nextPushedBackValules, values);
      }
      pushedBackValues.clear();
      Iterables.addAll(pushedBackValues, nextPushedBackValules);
    }

    doFnRunner.finishBundle();

    return outputManager.getOutputs();
  }

  private static class DoFnOutputManager implements DoFnRunners.OutputManager, Serializable {

    private List<RawUnionValue> outputs = Lists.newArrayList();
    private final TupleTag mainOutput;
    private final Map<TupleTag<?>, Integer> sideOutputsToTags;

    DoFnOutputManager(TupleTag mainOutput, Map<TupleTag<?>, Integer> sideOutputsToTags) {
      this.mainOutput = mainOutput;
      this.sideOutputsToTags = sideOutputsToTags;
    }

    @Override
    public <T> void output(TupleTag<T> outputTag, WindowedValue<T> output) {
      if (mainOutput.equals(outputTag)) {
        outputs.add(new RawUnionValue(0, output));
      } else {
        outputs.add(new RawUnionValue(sideOutputsToTags.get(outputTag), output));
      }
    }

    void clear() {
      outputs.clear();
    }

    Iterator<RawUnionValue> getOutputs() {
      return outputs.iterator();
    }
  }
}
