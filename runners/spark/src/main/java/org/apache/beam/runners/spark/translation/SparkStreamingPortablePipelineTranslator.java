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

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.stateful.SparkGroupAlsoByWindowViaWindowSet;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Translates a bounded portable pipeline into a Spark job. */
public class SparkStreamingPortablePipelineTranslator
    implements SparkPortablePipelineTranslator<SparkStreamingTranslationContext> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkStreamingPortablePipelineTranslator.class);

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
    //    translatorMap.put(
    //        ExecutableStage.URN,
    // SparkStreamingPortablePipelineTranslator::translateExecutableStage);
    //    translatorMap.put(
    //        PTransformTranslation.FLATTEN_TRANSFORM_URN,
    //        SparkStreamingPortablePipelineTranslator::translateFlatten);
    //    translatorMap.put(
    //        PTransformTranslation.RESHUFFLE_URN,
    //        SparkStreamingPortablePipelineTranslator::translateReshuffle);
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

    // create input DStream from empty RDD
    JavaRDD<WindowedValue<byte[]>> emptyRDD = context.getSparkContext().emptyRDD();
    Queue<JavaRDD<WindowedValue<byte[]>>> queueRDD = new LinkedBlockingQueue<>();
    queueRDD.add(emptyRDD);
    JavaInputDStream<WindowedValue<byte[]>> emptyStream =
        context.getStreamingContext().queueStream(queueRDD, true);
    UnboundedDataset<byte[]> output =
        new UnboundedDataset<>(
            emptyStream, Collections.singletonList(emptyStream.inputDStream().id()));
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

  private static String getInputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getInputsMap().values());
  }

  private static String getOutputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getOutputsMap().values());
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
