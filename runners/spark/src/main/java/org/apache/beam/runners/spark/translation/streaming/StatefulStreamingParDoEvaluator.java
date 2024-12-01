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
package org.apache.beam.runners.spark.translation.streaming;

import static org.apache.beam.runners.spark.translation.TranslationUtils.rejectTimers;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.stateful.StateAndTimers;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.ReifyTimestampsAndWindowsFunction;
import org.apache.beam.runners.spark.translation.SparkPCollectionView;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Option;
import scala.Tuple2;

/**
 * A specialized evaluator for ParDo operations in Spark Streaming context that is invoked when
 * stateful streaming is detected in the DoFn.
 *
 * <p>This class is used by {@link StreamingTransformTranslator}'s ParDo evaluator to handle
 * stateful streaming operations. When a DoFn contains stateful processing logic, the translation
 * process routes the execution through this evaluator instead of the standard ParDo evaluator.
 *
 * <p>The evaluator manages state handling and ensures proper processing semantics for streaming
 * stateful operations in the Spark runner context.
 *
 * <p>Important: This evaluator includes validation logic that rejects DoFn implementations
 * containing {@code @Timer} annotations, as timer functionality is not currently supported in the
 * Spark streaming context.
 */
public class StatefulStreamingParDoEvaluator<KeyT, ValueT, OutputT>
    implements TransformEvaluator<ParDo.MultiOutput<KV<KeyT, ValueT>, OutputT>> {

  @Override
  public void evaluate(
      ParDo.MultiOutput<KV<KeyT, ValueT>, OutputT> transform, EvaluationContext context) {
    final DoFn<KV<KeyT, ValueT>, OutputT> doFn = transform.getFn();
    final DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);

    rejectTimers(doFn);
    checkArgument(
        !signature.processElement().isSplittable(),
        "Splittable DoFn not yet supported in streaming mode: %s",
        doFn);
    checkState(
        signature.onWindowExpiration() == null, "onWindowExpiration is not supported: %s", doFn);

    // options, PCollectionView, WindowingStrategy
    final SerializablePipelineOptions options = context.getSerializableOptions();
    final SparkPCollectionView pviews = context.getPViews();
    final WindowingStrategy<?, ?> windowingStrategy =
        context.getInput(transform).getWindowingStrategy();

    final KvCoder<KeyT, ValueT> inputCoder =
        (KvCoder<KeyT, ValueT>) context.getInput(transform).getCoder();
    Map<TupleTag<?>, Coder<?>> outputCoders = context.getOutputCoders();
    JavaPairDStream<TupleTag<?>, WindowedValue<?>> all;

    final UnboundedDataset<KV<KeyT, ValueT>> unboundedDataset =
        (UnboundedDataset<KV<KeyT, ValueT>>) context.borrowDataset(transform);

    final JavaDStream<WindowedValue<KV<KeyT, ValueT>>> dStream = unboundedDataset.getDStream();

    final DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(context.getCurrentTransform());

    final Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(context.getCurrentTransform());

    final String stepName = context.getCurrentTransform().getFullName();

    final WindowFn<?, ?> windowFn = windowingStrategy.getWindowFn();

    final List<Integer> sourceIds = unboundedDataset.getStreamSources();

    // key, value coder
    final Coder<KeyT> keyCoder = inputCoder.getKeyCoder();
    final Coder<ValueT> valueCoder = inputCoder.getValueCoder();

    final WindowedValue.FullWindowedValueCoder<ValueT> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(valueCoder, windowFn.windowCoder());

    final MetricsContainerStepMapAccumulator metricsAccum = MetricsAccumulator.getInstance();
    final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs =
        TranslationUtils.getSideInputs(
            transform.getSideInputs().values(), context.getSparkContext(), pviews);

    final JavaPairDStream<
            /*Serialized KeyT*/ ByteArray, /*Serialized WindowedValue<ValueT>*/ byte[]>
        serializedDStream =
            dStream
                .map(new ReifyTimestampsAndWindowsFunction<>())
                .mapToPair(TranslationUtils.toPairFunction())
                .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));

    @SuppressWarnings({"rawtypes", "unchecked"})
    final JavaMapWithStateDStream<
            ByteArray, Option<byte[]>, State<StateAndTimers>, List<Tuple2<TupleTag<?>, byte[]>>>
        processedPairDStream =
            serializedDStream.mapWithState(
                StateSpec.function(
                    new ParDoStateUpdateFn<>(
                        metricsAccum,
                        stepName,
                        doFn,
                        keyCoder,
                        (WindowedValue.FullWindowedValueCoder) wvCoder,
                        options,
                        transform.getMainOutputTag(),
                        transform.getAdditionalOutputTags().getAll(),
                        inputCoder,
                        outputCoders,
                        sideInputs,
                        windowingStrategy,
                        doFnSchemaInformation,
                        sideInputMapping)));

    final JavaPairDStream<TupleTag<?>, byte[]> testPairDStream =
        processedPairDStream.flatMapToPair(List::iterator);

    all =
        testPairDStream.mapToPair(
            (Tuple2<TupleTag<?>, byte[]> e) -> {
              // deserialize
              final Coder<?> outputCoder = outputCoders.get(e._1());
              @SuppressWarnings("nullness")
              final WindowedValue<?> windowedValue =
                  CoderHelpers.fromByteArray(
                      e._2(),
                      WindowedValue.FullWindowedValueCoder.of(outputCoder, windowFn.windowCoder()));
              return Tuple2.apply(e._1(), windowedValue);
            });

    Map<TupleTag<?>, PCollection<?>> outputs = context.getOutputs(transform);
    if (hasMultipleOutputs(outputs)) {
      // Caching can cause Serialization, we need to code to bytes
      // more details in https://issues.apache.org/jira/browse/BEAM-2669
      Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap =
          TranslationUtils.getTupleTagCoders(outputs);
      all =
          all.mapToPair(TranslationUtils.getTupleTagEncodeFunction(coderMap))
              .cache()
              .mapToPair(TranslationUtils.getTupleTagDecodeFunction(coderMap));

      for (Map.Entry<TupleTag<?>, PCollection<?>> output : outputs.entrySet()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        JavaPairDStream<TupleTag<?>, WindowedValue<?>> filtered =
            all.filter(new TranslationUtils.TupleTagFilter(output.getKey()));
        @SuppressWarnings("unchecked")
        // Object is the best we can do since different outputs can have different tags
        JavaDStream<WindowedValue<Object>> values =
            (JavaDStream<WindowedValue<Object>>)
                (JavaDStream<?>) TranslationUtils.dStreamValues(filtered);
        context.putDataset(output.getValue(), new UnboundedDataset<>(values, sourceIds));
      }
    } else {
      @SuppressWarnings("unchecked")
      final JavaDStream<WindowedValue<Object>> values =
          (JavaDStream<WindowedValue<Object>>) (JavaDStream<?>) TranslationUtils.dStreamValues(all);

      context.putDataset(
          Iterables.getOnlyElement(outputs.entrySet()).getValue(),
          new UnboundedDataset<>(values, sourceIds));
    }
  }

  @Override
  public String toNativeString() {
    return "mapPartitions(new <fn>())";
  }

  private boolean hasMultipleOutputs(Map<TupleTag<?>, PCollection<?>> outputs) {
    return outputs.size() > 1;
  }
}
