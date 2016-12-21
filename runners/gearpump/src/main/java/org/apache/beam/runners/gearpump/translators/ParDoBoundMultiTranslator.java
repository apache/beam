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

package org.apache.beam.runners.gearpump.translators;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.DoFnRunnerFactory;
import org.apache.beam.runners.gearpump.translators.utils.NoOpAggregatorFactory;
import org.apache.beam.runners.gearpump.translators.utils.NoOpSideInputReader;
import org.apache.beam.runners.gearpump.translators.utils.NoOpStepContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FilterFunction;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;
import org.apache.gearpump.streaming.javaapi.dsl.functions.MapFunction;

/**
 * {@link ParDo.BoundMulti} is translated to Gearpump flatMap function
 * with {@link DoFn} wrapped in {@link DoFnMultiFunction}. The outputs are
 * further filtered with Gearpump filter function by output tag
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ParDoBoundMultiTranslator<InputT, OutputT> implements
    TransformTranslator<ParDo.BoundMulti<InputT, OutputT>> {

  @Override
  public void translate(ParDo.BoundMulti<InputT, OutputT> transform, TranslationContext context) {
    PCollection<InputT> inputT = (PCollection<InputT>) context.getInput(transform);
    JavaStream<WindowedValue<InputT>> inputStream = context.getInputStream(inputT);
    Map<TupleTag<?>, PCollection<?>> outputs = context.getOutput(transform).getAll();

    JavaStream<WindowedValue<KV<TupleTag<OutputT>, OutputT>>> outputStream = inputStream.flatMap(
        new DoFnMultiFunction<>(
            context.getPipelineOptions(),
            transform.getFn(),
            transform.getMainOutputTag(),
            transform.getSideOutputTags(),
            inputT.getWindowingStrategy(),
            new NoOpSideInputReader()
        ), transform.getName());
    for (Map.Entry<TupleTag<?>, PCollection<?>> output : outputs.entrySet()) {
      JavaStream<WindowedValue<OutputT>> taggedStream = outputStream
          .filter(new FilterByOutputTag<>((TupleTag<OutputT>) output.getKey())
              , "filter_by_output_tag")
          .map(new ExtractOutput<OutputT>(), "extract output");

      context.setOutputStream(output.getValue(), taggedStream);
    }
  }

  /**
   * Gearpump {@link FlatMapFunction} wrapper over Beam {@link DoFnMultiFunction}.
   */
  private static class DoFnMultiFunction<InputT, OutputT> implements
      FlatMapFunction<WindowedValue<InputT>, WindowedValue<KV<TupleTag<OutputT>, OutputT>>>,
      DoFnRunners.OutputManager {

    private final DoFnRunnerFactory<InputT, OutputT> doFnRunnerFactory;
    private DoFnRunner<InputT, OutputT> doFnRunner;
    private final List<WindowedValue<KV<TupleTag<OutputT>, OutputT>>> outputs = Lists
        .newArrayList();

    public DoFnMultiFunction(
        GearpumpPipelineOptions pipelineOptions,
        DoFn<InputT, OutputT> doFn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList sideOutputTags,
        WindowingStrategy<?, ?> windowingStrategy,
        SideInputReader sideInputReader) {
      this.doFnRunnerFactory = new DoFnRunnerFactory<>(
          pipelineOptions,
          doFn,
          sideInputReader,
          this,
          mainOutputTag,
          sideOutputTags.getAll(),
          new NoOpStepContext(),
          new NoOpAggregatorFactory(),
          windowingStrategy
      );
    }

    @Override
    public Iterator<WindowedValue<KV<TupleTag<OutputT>, OutputT>>> apply(WindowedValue<InputT> wv) {
      if (null == doFnRunner) {
        doFnRunner = doFnRunnerFactory.createRunner();
      }
      doFnRunner.startBundle();
      doFnRunner.processElement(wv);
      doFnRunner.finishBundle();

      return outputs.iterator();
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      KV<TupleTag<OutputT>, OutputT> kv = KV.of((TupleTag<OutputT>) tag,
          (OutputT) output.getValue());
      outputs.add(WindowedValue.of(kv, output.getTimestamp(),
          output.getWindows(), output.getPane()));
    }
  }

  private static class FilterByOutputTag<OutputT> implements
      FilterFunction<WindowedValue<KV<TupleTag<OutputT>, OutputT>>> {

    private final TupleTag<OutputT> tupleTag;

    public FilterByOutputTag(TupleTag<OutputT> tupleTag) {
      this.tupleTag = tupleTag;
    }

    @Override
    public boolean apply(WindowedValue<KV<TupleTag<OutputT>, OutputT>> wv) {
      return wv.getValue().getKey().equals(tupleTag);
    }
  }

  private static class ExtractOutput<OutputT> implements
      MapFunction<WindowedValue<KV<TupleTag<OutputT>, OutputT>>, WindowedValue<OutputT>> {

    @Override
    public WindowedValue<OutputT> apply(WindowedValue<KV<TupleTag<OutputT>, OutputT>> wv) {
      return WindowedValue.of(wv.getValue().getValue(), wv.getTimestamp(),
          wv.getWindows(), wv.getPane());
    }
  }
}
