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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.createOutputMap;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.SparkCompat;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/** Translates a bounded portable pipeline into a Spark job. */
public class SparkStreamingPortablePipelineTranslator
    implements SparkPortablePipelineTranslator<SparkStreamingTranslationContext> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkStreamingPortablePipelineTranslator.class);

  private static Instant firstTimestamp;

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  interface PTransformTranslator {

    /** Translates transformNode from Beam into the Spark context. */
    void translate(
        PTransformNode transformNode,
        RunnerApi.Pipeline pipeline,
        SparkStreamingTranslationContext context);
  }

  @Override
  public Set<String> knownUrns() {
    return urnToTransformTranslator.keySet();
  }

  public SparkStreamingPortablePipelineTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator> translatorMap = ImmutableMap.builder();
    translatorMap.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        SparkStreamingPortablePipelineTranslator::translateImpulse);
    translatorMap.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        SparkStreamingPortablePipelineTranslator::translateGroupByKey);
    translatorMap.put(
        ExecutableStage.URN, SparkStreamingPortablePipelineTranslator::translateExecutableStage);
    translatorMap.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN,
        SparkStreamingPortablePipelineTranslator::translateFlatten);
    translatorMap.put(
        PTransformTranslation.RESHUFFLE_URN,
        SparkStreamingPortablePipelineTranslator::translateReshuffle);
    this.urnToTransformTranslator = translatorMap.build();
  }

  /** Translates pipeline from Beam into the Spark context. */
  @Override
  public void translate(
      final RunnerApi.Pipeline pipeline, SparkStreamingTranslationContext context) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transformNode : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(
              transformNode.getTransform().getSpec().getUrn(),
              SparkStreamingPortablePipelineTranslator::urnNotFound)
          .translate(transformNode, pipeline, context);
    }
  }

  private static void urnNotFound(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Transform %s has unknown URN %s",
            transformNode.getId(), transformNode.getTransform().getSpec().getUrn()));
  }

  private static void translateImpulse(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {

    TimestampedValue<byte[]> tsValue = TimestampedValue.atMinimumTimestamp(new byte[0]);
    Iterable<TimestampedValue<byte[]>> timestampedValues = Collections.singletonList(tsValue);
    Iterable<WindowedValue<byte[]>> windowedValues =
        StreamSupport.stream(timestampedValues.spliterator(), false)
            .map(
                timestampedValue ->
                    WindowedValue.of(
                        timestampedValue.getValue(),
                        timestampedValue.getTimestamp(),
                        GlobalWindow.INSTANCE,
                        PaneInfo.NO_FIRING))
            .collect(Collectors.toList());

    ByteArrayCoder coder = ByteArrayCoder.of();

    WindowedValue.FullWindowedValueCoder<byte[]> windowCoder =
        WindowedValue.FullWindowedValueCoder.of(coder, GlobalWindow.Coder.INSTANCE);
    JavaRDD<WindowedValue<byte[]>> emptyRDD =
        context
            .getSparkContext()
            .parallelize(CoderHelpers.toByteArrays(windowedValues, windowCoder))
            .map(CoderHelpers.fromByteFunction(windowCoder));

    // create input DStream from RDD queue
    Queue<JavaRDD<WindowedValue<byte[]>>> queueRDD = new LinkedBlockingQueue<>();
    queueRDD.offer(emptyRDD);
    JavaInputDStream<WindowedValue<byte[]>> emptyStream =
        context.getStreamingContext().queueStream(queueRDD, true);

    UnboundedDataset<byte[]> output =
        new UnboundedDataset<>(
            emptyStream, Collections.singletonList(emptyStream.inputDStream().id()));

    if (firstTimestamp == null) firstTimestamp = new Instant();
    GlobalWatermarkHolder.SparkWatermarks sparkWatermark =
        new GlobalWatermarkHolder.SparkWatermarks(
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            firstTimestamp);
    GlobalWatermarkHolder.add(output.getStreamSources().get(0), sparkWatermark);

    context.pushDataset(getOutputId(transformNode), output);
  }

  private static <K, V> void translateGroupByKey(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();
    String inputId = getInputId(transformNode);
    UnboundedDataset<KV<K, V>> inputDataset =
        (UnboundedDataset<KV<K, V>>) context.popDataset(inputId);
    List<Integer> streamSources = inputDataset.getStreamSources();
    JavaDStream<WindowedValue<KV<K, V>>> dStream = inputDataset.getDStream();
    WindowedValue.WindowedValueCoder<KV<K, V>> inputCoder =
        getWindowedValueCoder(inputId, components);
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
    Coder<K> inputKeyCoder = inputKvCoder.getKeyCoder();
    Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
    final WindowingStrategy windowingStrategy = getWindowingStrategy(inputId, components);
    final WindowFn<Object, BoundedWindow> windowFn = windowingStrategy.getWindowFn();
    final WindowedValue.WindowedValueCoder<V> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(inputValueCoder, windowFn.windowCoder());

    JavaDStream<WindowedValue<KV<K, Iterable<V>>>> outStream =
        SparkGroupAlsoByWindowViaWindowSet.groupByKeyAndWindow(
            dStream,
            inputKeyCoder,
            wvCoder,
            windowingStrategy,
            context.getSerializableOptions(),
            streamSources,
            transformNode.getId());

    context.pushDataset(
        getOutputId(transformNode), new UnboundedDataset<>(outStream, streamSources));
  }

  private static <InputT, OutputT, SideInputT> void translateExecutableStage(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {
    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transformNode.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId = stagePayload.getInput();
    UnboundedDataset<InputT> inputDataset =
        (UnboundedDataset<InputT>) context.popDataset(inputPCollectionId);
    List<Integer> streamSources = inputDataset.getStreamSources();
    JavaDStream<WindowedValue<InputT>> inputDStream = inputDataset.getDStream();
    Map<String, String> outputs = transformNode.getTransform().getOutputsMap();
    BiMap<String, Integer> outputMap = createOutputMap(outputs.values());

    RunnerApi.Components components = pipeline.getComponents();
    Coder windowCoder =
        getWindowingStrategy(inputPCollectionId, components).getWindowFn().windowCoder();

    // TODO: handle side inputs?
    ImmutableMap<
            String, Tuple2<Broadcast<List<byte[]>>, WindowedValue.WindowedValueCoder<SideInputT>>>
        broadcastVariables = ImmutableMap.copyOf(new HashMap<>());

    SparkExecutableStageFunction<InputT, SideInputT> function =
        new SparkExecutableStageFunction<>(
            stagePayload,
            context.jobInfo,
            outputMap,
            SparkExecutableStageContextFactory.getInstance(),
            broadcastVariables,
            MetricsAccumulator.getInstance(),
            windowCoder);
    JavaDStream<RawUnionValue> staged = inputDStream.mapPartitions(function);

    String intermediateId = getExecutableStageIntermediateId(transformNode);
    context.pushDataset(
        intermediateId,
        new Dataset() {
          @Override
          public void cache(String storageLevel, Coder<?> coder) {
            StorageLevel level = StorageLevel.fromString(storageLevel);
            staged.persist(level);
          }

          @Override
          public void action() {
            // Empty function to force computation of RDD.
            staged.foreachRDD(TranslationUtils.emptyVoidFunction());
          }

          @Override
          public void setName(String name) {
            // ignore
          }
        });
    // pop dataset to mark RDD as used
    context.popDataset(intermediateId);

    for (String outputId : outputs.values()) {
      JavaDStream<WindowedValue<OutputT>> outStream =
          staged.flatMap(new SparkExecutableStageExtractionFunction<>(outputMap.get(outputId)));
      context.pushDataset(outputId, new UnboundedDataset<>(outStream, streamSources));
    }

    if (outputs.isEmpty()) {
      // Add sink to ensure all outputs are computed
      JavaDStream<WindowedValue<OutputT>> outStream =
          staged.flatMap((rawUnionValue) -> Collections.emptyIterator());
      context.pushDataset(
          String.format("EmptyOutputSink_%d", context.nextSinkId()),
          new UnboundedDataset<>(outStream, streamSources));
    }
  }

  private static <T> void translateFlatten(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {
    Map<String, String> inputsMap = transformNode.getTransform().getInputsMap();
    final List<JavaDStream<WindowedValue<T>>> dStreams = new ArrayList<>();
    final List<Integer> streamingSources = new ArrayList<>();
    for (String inputId : inputsMap.values()) {
      Dataset dataset = context.popDataset(inputId);
      if (dataset instanceof UnboundedDataset) {
        UnboundedDataset<T> unboundedDataset = (UnboundedDataset<T>) dataset;
        streamingSources.addAll(unboundedDataset.getStreamSources());
        dStreams.add(unboundedDataset.getDStream());
      } else {
        // create a single RDD stream.
        Queue<JavaRDD<WindowedValue<T>>> q = new LinkedBlockingQueue<>();
        q.offer(((BoundedDataset) dataset).getRDD());
        // TODO: this is not recoverable from checkpoint!
        JavaDStream<WindowedValue<T>> dStream = context.getStreamingContext().queueStream(q);
        dStreams.add(dStream);
      }
    }
    // Unify streams into a single stream.
    JavaDStream<WindowedValue<T>> unifiedStreams =
        SparkCompat.joinStreams(context.getStreamingContext(), dStreams);
    context.pushDataset(
        getOutputId(transformNode), new UnboundedDataset<>(unifiedStreams, streamingSources));
  }

  private static <T> void translateReshuffle(
      PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      SparkStreamingTranslationContext context) {
    String inputId = getInputId(transformNode);
    UnboundedDataset<T> inputDataset =
        (UnboundedDataset<T>) context.popDataset(inputId);
    List<Integer> streamSources = inputDataset.getStreamSources();
    JavaDStream<WindowedValue<T>> dStream = inputDataset.getDStream();
    WindowedValue.WindowedValueCoder<T> coder = getWindowedValueCoder(inputId, pipeline.getComponents());

    JavaDStream<WindowedValue<T>> reshuffledStream =
        dStream.transform(rdd -> GroupCombineFunctions.reshuffle(rdd, coder));

    context.pushDataset(getOutputId(transformNode), new UnboundedDataset<>(reshuffledStream, streamSources));
  }

  private static String getInputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getInputsMap().values());
  }

  private static String getOutputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getOutputsMap().values());
  }

  private static String getExecutableStageIntermediateId(PTransformNode transformNode) {
    return transformNode.getId();
  }

  private static <T> WindowedValue.WindowedValueCoder<T> getWindowedValueCoder(
      String pCollectionId, RunnerApi.Components components) {
    RunnerApi.PCollection pCollection = components.getPcollectionsOrThrow(pCollectionId);
    PipelineNode.PCollectionNode pCollectionNode =
        PipelineNode.pCollection(pCollectionId, pCollection);
    WindowedValue.WindowedValueCoder<T> coder;
    try {
      coder =
          (WindowedValue.WindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(pCollectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return coder;
  }

  @Override
  public SparkStreamingTranslationContext createTranslationContext(
      JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo) {
    return new SparkStreamingTranslationContext(jsc, options, jobInfo);
  }
}
