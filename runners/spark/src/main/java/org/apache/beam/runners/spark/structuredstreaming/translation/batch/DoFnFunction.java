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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.NoOpStepContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SparkSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.CachedSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.LinkedListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import scala.Tuple2;

/**
 * Encapsulates a {@link DoFn} inside a Spark {@link
 * org.apache.spark.api.java.function.MapPartitionsFunction}.
 *
 * <p>We get a mapping from {@link org.apache.beam.sdk.values.TupleTag} to output index and must tag
 * all outputs with the output number. Afterwards a filter will filter out those elements that are
 * not to be in a specific output.
 */
public class DoFnFunction<InputT, OutputT>
    implements MapPartitionsFunction<WindowedValue<InputT>, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final MetricsContainerStepMapAccumulator metricsAccum;
  private final String stepName;
  private final DoFn<InputT, OutputT> doFn;
  private transient boolean wasSetupCalled;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;
  private final SerializablePipelineOptions serializableOptions;
  private final List<TupleTag<?>> additionalOutputTags;
  private final TupleTag<OutputT> mainOutputTag;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoderMap;
  private final SideInputBroadcast broadcastStateData;
  private DoFnSchemaInformation doFnSchemaInformation;
  private Map<String, PCollectionView<?>> sideInputMapping;

  public DoFnFunction(
      MetricsContainerStepMapAccumulator metricsAccum,
      String stepName,
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      SerializablePipelineOptions serializableOptions,
      List<TupleTag<?>> additionalOutputTags,
      TupleTag<OutputT> mainOutputTag,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoderMap,
      SideInputBroadcast broadcastStateData,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.metricsAccum = metricsAccum;
    this.stepName = stepName;
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.serializableOptions = serializableOptions;
    this.additionalOutputTags = additionalOutputTags;
    this.mainOutputTag = mainOutputTag;
    this.inputCoder = inputCoder;
    this.outputCoderMap = outputCoderMap;
    this.broadcastStateData = broadcastStateData;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> call(Iterator<WindowedValue<InputT>> iter)
      throws Exception {
    if (!wasSetupCalled && iter.hasNext()) {
      DoFnInvokers.tryInvokeSetupFor(doFn);
      wasSetupCalled = true;
    }

    DoFnOutputManager outputManager = new DoFnOutputManager();

    DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            serializableOptions.get(),
            doFn,
            CachedSideInputReader.of(new SparkSideInputReader(sideInputs, broadcastStateData)),
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            new NoOpStepContext(),
            inputCoder,
            outputCoderMap,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    DoFnRunnerWithMetrics<InputT, OutputT> doFnRunnerWithMetrics =
        new DoFnRunnerWithMetrics<>(stepName, doFnRunner, metricsAccum);

    return new ProcessContext<>(
            doFn, doFnRunnerWithMetrics, outputManager, Collections.emptyIterator())
        .processPartition(iter)
        .iterator();
  }

  private class DoFnOutputManager
      implements ProcessContext.ProcessOutputManager<Tuple2<TupleTag<?>, WindowedValue<?>>> {

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
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }
}
