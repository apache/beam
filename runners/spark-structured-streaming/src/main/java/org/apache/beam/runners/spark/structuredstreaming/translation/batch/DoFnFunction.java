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

package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkNoOpStepContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DoFnFunction<InputT, OutputT>
    implements MapPartitionsFunction<WindowedValue<InputT>, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final SerializablePipelineOptions serializedOptions;

  private final DoFn<InputT, OutputT> doFn;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  private final WindowingStrategy<?, ?> windowingStrategy;

  private final Map<TupleTag<?>, Integer> outputMap;
  private final TupleTag<OutputT> mainOutputTag;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoderMap;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  public DoFnFunction(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options,
      Map<TupleTag<?>, Integer> outputMap,
      TupleTag<OutputT> mainOutputTag,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoderMap) {

    this.doFn = doFn;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputMap = outputMap;
    this.mainOutputTag = mainOutputTag;
    this.inputCoder = inputCoder;
    this.outputCoderMap = outputCoderMap;
  }

  @Override
  public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> call(Iterator<WindowedValue<InputT>> iter)
      throws Exception {

    DoFnOutputManager outputManager = new DoFnOutputManager();

    List<TupleTag<?>> additionalOutputTags = Lists.newArrayList(outputMap.keySet());

    DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            serializedOptions.get(),
            doFn,
            new SparkSideInputReader(sideInputs),
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            new SparkNoOpStepContext(),
            inputCoder,
            outputCoderMap,
            windowingStrategy);

    return new SparkProcessContext<>(doFn, doFnRunner, outputManager, Collections.emptyIterator())
        .processPartition(iter)
        .iterator();
  }

  private class DoFnOutputManager
      implements SparkProcessContext.SparkOutputManager<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    @Override
    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      Iterator<Map.Entry<TupleTag<?>, WindowedValue<?>>> entryIter = outputs.entries().iterator();
      return Iterators.transform(entryIter, this.entryToTupleFn());
    }

    private <K, V> Function<Map.Entry<K, V>, Tuple2<K, V>> entryToTupleFn() {
      return en -> new Tuple2<>(en.getKey(), en.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }
}
