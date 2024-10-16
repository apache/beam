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
package org.apache.beam.runners.flink;

import static java.lang.String.format;
import static org.apache.beam.sdk.util.construction.SplittableParDo.SPLITTABLE_PROCESS_URN;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.functions.ImpulseSourceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToFlinkKeyKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SplittableDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.BeamStoppableFunction;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.DedupingOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestStreamSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSource;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.ReadTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class contains all the mappings between Beam and Flink <b>streaming</b> transformations. The
 * {@link FlinkStreamingPipelineTranslator} traverses the Beam job and comes here to translate the
 * encountered Beam transformations into Flink one, based on the mapping available in this class.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkStreamingTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  /** A map from a Transform URN to the translator. */
  @SuppressWarnings("rawtypes")
  private static final Map<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator>
      TRANSLATORS = new HashMap<>();

  // here you can find all the available translators.
  static {
    TRANSLATORS.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator());
    TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator());

    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoStreamingTranslator());
    TRANSLATORS.put(SPLITTABLE_PROCESS_URN, new SplittableProcessElementsStreamingTranslator());
    TRANSLATORS.put(SplittableParDo.SPLITTABLE_GBKIKWI_URN, new GBKIntoKeyedWorkItemsTranslator());

    TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator());
    TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionTranslator());
    TRANSLATORS.put(
        CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN,
        new CreateViewStreamingTranslator());

    TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslatorStreaming());
    TRANSLATORS.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
    TRANSLATORS.put(
        PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new CombinePerKeyTranslator());

    TRANSLATORS.put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator());
  }

  private static final String FORCED_SLOT_GROUP = "beam";

  public static FlinkStreamingPipelineTranslator.StreamTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform) {
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSLATORS.get(urn);
  }

  @SuppressWarnings("unchecked")
  public static String getCurrentTransformName(FlinkStreamingTranslationContext context) {
    return context.getCurrentTransform().getFullName();
  }

  // --------------------------------------------------------------------------------------------
  //  Transformation Implementations
  // --------------------------------------------------------------------------------------------

  private static class UnboundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<T>> transform, FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      DataStream<WindowedValue<T>> source;
      DataStream<WindowedValue<ValueWithRecordId<T>>> nonDedupSource;
      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      Coder<T> coder = context.getOutput(transform).getCoder();

      TypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(
                  ValueWithRecordId.ValueWithRecordIdCoder.of(coder),
                  output.getWindowingStrategy().getWindowFn().windowCoder()),
              context.getPipelineOptions());

      UnboundedSource<T, ?> rawSource;
      try {
        rawSource =
            ReadTranslation.unboundedSourceFromTransform(
                (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
                    context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String fullName = getCurrentTransformName(context);
      try {
        int parallelism =
            context.getExecutionEnvironment().getMaxParallelism() > 0
                ? context.getExecutionEnvironment().getMaxParallelism()
                : context.getExecutionEnvironment().getParallelism();

        FlinkUnboundedSource<T> unboundedSource =
            FlinkSource.unbounded(
                transform.getName(),
                rawSource,
                new SerializablePipelineOptions(context.getPipelineOptions()),
                parallelism);
        nonDedupSource =
            context
                .getExecutionEnvironment()
                .fromSource(
                    unboundedSource, WatermarkStrategy.noWatermarks(), fullName, withIdTypeInfo)
                .uid(fullName);

        if (rawSource.requiresDeduping()) {
          source =
              nonDedupSource
                  .keyBy(new ValueWithRecordIdKeySelector<>())
                  .transform(
                      "deduping",
                      outputTypeInfo,
                      new DedupingOperator<>(context.getPipelineOptions()))
                  .uid(format("%s/__deduplicated__", fullName));
        } else {
          source =
              nonDedupSource
                  .flatMap(new StripIdsMap<>(context.getPipelineOptions()))
                  .returns(outputTypeInfo);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error while translating UnboundedSource: " + rawSource, e);
      }

      context.setOutputDataStream(output, source);
    }
  }

  static class ValueWithRecordIdKeySelector<T>
      implements KeySelector<WindowedValue<ValueWithRecordId<T>>, FlinkKey>,
          ResultTypeQueryable<FlinkKey> {

    @Override
    public FlinkKey getKey(WindowedValue<ValueWithRecordId<T>> value) throws Exception {
      return FlinkKey.of(ByteBuffer.wrap(value.getValue().getId()));
    }

    @Override
    public TypeInformation<FlinkKey> getProducedType() {
      return ValueTypeInfo.of(FlinkKey.class);
    }
  }

  public static class StripIdsMap<T>
      extends RichFlatMapFunction<WindowedValue<ValueWithRecordId<T>>, WindowedValue<T>> {

    private final SerializablePipelineOptions options;

    StripIdsMap(PipelineOptions options) {
      this.options = new SerializablePipelineOptions(options);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public void flatMap(
        WindowedValue<ValueWithRecordId<T>> value, Collector<WindowedValue<T>> collector)
        throws Exception {
      collector.collect(value.withValue(value.getValue().getValue()));
    }
  }

  private static class ImpulseTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<Impulse> {
    @Override
    void translateNode(Impulse transform, FlinkStreamingTranslationContext context) {

      TypeInformation<WindowedValue<byte[]>> typeInfo =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE),
              context.getPipelineOptions());

      SingleOutputStreamOperator<WindowedValue<byte[]>> impulseOperator;
      if (context.isStreaming()) {
        long shutdownAfterIdleSourcesMs =
            context
                .getPipelineOptions()
                .as(FlinkPipelineOptions.class)
                .getShutdownSourcesAfterIdleMs();
        impulseOperator =
            context
                .getExecutionEnvironment()
                .addSource(new ImpulseSourceFunction(shutdownAfterIdleSourcesMs), "Impulse")
                .returns(typeInfo);
      } else {
        FlinkBoundedSource<byte[]> impulseSource = FlinkSource.boundedImpulse();
        impulseOperator =
            context
                .getExecutionEnvironment()
                .fromSource(impulseSource, WatermarkStrategy.noWatermarks(), "Impulse")
                .returns(typeInfo);

        if (!context.isStreaming()
            && context
                .getPipelineOptions()
                .as(FlinkPipelineOptions.class)
                .getForceSlotSharingGroup()) {
          impulseOperator = impulseOperator.slotSharingGroup(FORCED_SLOT_GROUP);
        }
      }
      context.setOutputDataStream(context.getOutput(transform), impulseOperator);
    }
  }

  private static class ReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    private final BoundedReadSourceTranslator<T> boundedTranslator =
        new BoundedReadSourceTranslator<>();
    private final UnboundedReadSourceTranslator<T> unboundedTranslator =
        new UnboundedReadSourceTranslator<>();

    @Override
    void translateNode(
        PTransform<PBegin, PCollection<T>> transform, FlinkStreamingTranslationContext context) {
      if (ReadTranslation.sourceIsBounded(context.getCurrentTransform())
          == PCollection.IsBounded.BOUNDED) {
        boundedTranslator.translateNode(transform, context);
      } else {
        unboundedTranslator.translateNode(transform, context);
      }
    }
  }

  private static class BoundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<T>> transform, FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      BoundedSource<T> rawSource;
      try {
        rawSource =
            ReadTranslation.boundedSourceFromTransform(
                (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
                    context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      String fullName = getCurrentTransformName(context);
      int parallelism =
          context.getExecutionEnvironment().getMaxParallelism() > 0
              ? context.getExecutionEnvironment().getMaxParallelism()
              : context.getExecutionEnvironment().getParallelism();

      FlinkBoundedSource<T> flinkBoundedSource =
          FlinkSource.bounded(
              transform.getName(),
              rawSource,
              new SerializablePipelineOptions(context.getPipelineOptions()),
              parallelism);

      TypeInformation<WindowedValue<T>> typeInfo = context.getTypeInfo(output);

      SingleOutputStreamOperator<WindowedValue<T>> source;
      try {
        source =
            context
                .getExecutionEnvironment()
                .fromSource(
                    flinkBoundedSource, WatermarkStrategy.noWatermarks(), fullName, outputTypeInfo)
                .uid(fullName)
                .returns(typeInfo);

        if (!context.isStreaming()
            && context
                .getPipelineOptions()
                .as(FlinkPipelineOptions.class)
                .getForceSlotSharingGroup()) {
          source = source.slotSharingGroup(FORCED_SLOT_GROUP);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error while translating BoundedSource: " + rawSource, e);
      }
      context.setOutputDataStream(output, source);
    }
  }

  /** Wraps each element in a {@link RawUnionValue} with the given tag id. */
  public static class ToRawUnion<T> extends RichMapFunction<T, RawUnionValue> {
    private final int intTag;
    private final SerializablePipelineOptions options;

    ToRawUnion(int intTag, PipelineOptions pipelineOptions) {
      this.intTag = intTag;
      this.options = new SerializablePipelineOptions(pipelineOptions);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public RawUnionValue map(T o) throws Exception {
      return new RawUnionValue(intTag, o);
    }
  }

  public static Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>>
      transformSideInputs(
          Collection<PCollectionView<?>> sideInputs, FlinkStreamingTranslationContext context) {

    // collect all side inputs
    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    int count = 0;
    for (PCollectionView<?> sideInput : sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      intToViewMapping.put(count, sideInput);
      tagToIntMapping.put(tag, count);
      count++;
    }

    List<Coder<?>> inputCoders = new ArrayList<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException("Input Stream TypeInformation is no CoderTypeInformation.");
      }

      Coder<?> coder = ((CoderTypeInformation) tpe).getCoder();
      inputCoders.add(coder);
    }

    UnionCoder unionCoder = UnionCoder.of(inputCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
        new CoderTypeInformation<>(unionCoder, context.getPipelineOptions());

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (PCollectionView<?> sideInput : sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      DataStream<RawUnionValue> unionValueStream =
          sideInputStream
              .map(new ToRawUnion<>(intTag, context.getPipelineOptions()))
              .returns(unionTypeInformation);

      if (sideInputUnion == null) {
        sideInputUnion = unionValueStream;
      } else {
        sideInputUnion = sideInputUnion.union(unionValueStream);
      }
    }

    if (sideInputUnion == null) {
      throw new IllegalStateException("No unioned side inputs, this indicates a bug.");
    }

    return new Tuple2<>(intToViewMapping, sideInputUnion);
  }

  /**
   * Helper for translating {@code ParDo.MultiOutput} and {@link
   * SplittableParDoViaKeyedWorkItems.ProcessElements}.
   */
  static class ParDoTranslationHelper {

    interface DoFnOperatorFactory<InputT, OutputT> {
      DoFnOperator<InputT, InputT, OutputT> createDoFnOperator(
          DoFn<InputT, OutputT> doFn,
          String stepName,
          List<PCollectionView<?>> sideInputs,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          FlinkStreamingTranslationContext context,
          WindowingStrategy<?, ?> windowingStrategy,
          Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
          Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
          Map<TupleTag<?>, Integer> tagsToIds,
          Coder<WindowedValue<InputT>> windowedInputCoder,
          Map<TupleTag<?>, Coder<?>> outputCoders,
          Coder keyCoder,
          KeySelector<WindowedValue<InputT>, ?> keySelector,
          Map<Integer, PCollectionView<?>> transformedSideInputs,
          DoFnSchemaInformation doFnSchemaInformation,
          Map<String, PCollectionView<?>> sideInputMapping);
    }

    static <InputT, OutputT> void translateParDo(
        String transformName,
        DoFn<InputT, OutputT> doFn,
        PCollection<InputT> input,
        List<PCollectionView<?>> sideInputs,
        Map<TupleTag<?>, PCollection<?>> outputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<String, PCollectionView<?>> sideInputMapping,
        FlinkStreamingTranslationContext context,
        DoFnOperatorFactory<InputT, OutputT> doFnOperatorFactory) {

      // we assume that the transformation does not change the windowing strategy.
      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

      Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newHashMap();
      Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newHashMap();

      // We associate output tags with ids, the Integer is easier to serialize than TupleTag.
      // The return map of AppliedPTransform.getOutputs() is an ImmutableMap, its implementation is
      // RegularImmutableMap, its entrySet order is the same with the order of insertion.
      // So we can use the original AppliedPTransform.getOutputs() to produce deterministic ids.
      Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
      int idCount = 0;
      tagsToIds.put(mainOutputTag, idCount++);
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : outputs.entrySet()) {
        if (!tagsToOutputTags.containsKey(entry.getKey())) {
          tagsToOutputTags.put(
              entry.getKey(),
              new OutputTag<WindowedValue<?>>(
                  entry.getKey().getId(),
                  (TypeInformation) context.getTypeInfo((PCollection<?>) entry.getValue())));
          tagsToCoders.put(
              entry.getKey(),
              (Coder) context.getWindowedInputCoder((PCollection<OutputT>) entry.getValue()));
          tagsToIds.put(entry.getKey(), idCount++);
        }
      }

      SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;

      Coder<WindowedValue<InputT>> windowedInputCoder = context.getWindowedInputCoder(input);
      Map<TupleTag<?>, Coder<?>> outputCoders = context.getOutputCoders();

      DataStream<WindowedValue<InputT>> inputDataStream = context.getInputDataStream(input);

      Coder keyCoder = null;
      KeySelector<WindowedValue<InputT>, ?> keySelector = null;
      boolean stateful = false;
      DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
      if (!signature.stateDeclarations().isEmpty()
          || !signature.timerDeclarations().isEmpty()
          || !signature.timerFamilyDeclarations().isEmpty()) {
        // Based on the fact that the signature is stateful, DoFnSignatures ensures
        // that it is also keyed
        keyCoder = ((KvCoder) input.getCoder()).getKeyCoder();
        keySelector =
            new KvToFlinkKeyKeySelector<>(keyCoder);
        final PTransform<?, PCollection<InputT>> producer = context.getProducer(input);
        final String previousUrn =
            producer != null
                ? PTransformTranslation.urnForTransformOrNull(context.getProducer(input))
                : null;
        // We can skip reshuffle in case previous transform was CPK or GBK
        if (PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN.equals(previousUrn)
            || PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN.equals(previousUrn)) {
          inputDataStream = DataStreamUtils.reinterpretAsKeyedStream(inputDataStream, keySelector);
        } else {
          inputDataStream = inputDataStream.keyBy(keySelector);
        }
        stateful = true;
      } else if (doFn instanceof SplittableParDoViaKeyedWorkItems.ProcessFn) {
        // we know that it is keyed on byte[]
        keyCoder = ByteArrayCoder.of();
        keySelector =
            new WorkItemKeySelector<>(keyCoder);
        stateful = true;
      }

      CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation =
          new CoderTypeInformation<>(
              context.getWindowedInputCoder((PCollection<OutputT>) outputs.get(mainOutputTag)),
              context.getPipelineOptions());

      if (sideInputs.isEmpty()) {
        DoFnOperator<InputT, InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                getCurrentTransformName(context),
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToOutputTags,
                tagsToCoders,
                tagsToIds,
                windowedInputCoder,
                outputCoders,
                keyCoder,
                keySelector,
                new HashMap<>() /* side-input mapping */,
                doFnSchemaInformation,
                sideInputMapping);

        outputStream =
            inputDataStream.transform(transformName, outputTypeInformation, doFnOperator);

      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformedSideInputs =
            transformSideInputs(sideInputs, context);

        DoFnOperator<InputT, InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                getCurrentTransformName(context),
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToOutputTags,
                tagsToCoders,
                tagsToIds,
                windowedInputCoder,
                outputCoders,
                keyCoder,
                keySelector,
                transformedSideInputs.f0,
                doFnSchemaInformation,
                sideInputMapping);

        if (stateful) {
          // we have to manually construct the two-input transform because we're not
          // allowed to have only one input keyed, normally.
          KeyedStream keyedStream = (KeyedStream<?, InputT>) inputDataStream;
          TwoInputTransformation<
                  WindowedValue<KV<?, InputT>>, RawUnionValue, WindowedValue<OutputT>>
              rawFlinkTransform =
                  new TwoInputTransformation(
                      keyedStream.getTransformation(),
                      transformedSideInputs.f1.broadcast().getTransformation(),
                      transformName,
                      doFnOperator,
                      outputTypeInformation,
                      keyedStream.getParallelism());

          rawFlinkTransform.setStateKeyType(keyedStream.getKeyType());
          rawFlinkTransform.setStateKeySelectors(keyedStream.getKeySelector(), null);

          outputStream =
              new SingleOutputStreamOperator(
                  keyedStream.getExecutionEnvironment(),
                  rawFlinkTransform) {}; // we have to cheat around the ctor being protected

          keyedStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

        } else {
          outputStream =
              inputDataStream
                  .connect(transformedSideInputs.f1.broadcast())
                  .transform(transformName, outputTypeInformation, doFnOperator);
        }
      }

      outputStream.uid(transformName);
      context.setOutputDataStream(outputs.get(mainOutputTag), outputStream);

      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : outputs.entrySet()) {
        if (!entry.getKey().equals(mainOutputTag)) {
          context.setOutputDataStream(
              entry.getValue(), outputStream.getSideOutput(tagsToOutputTags.get(entry.getKey())));
        }
      }
    }
  }

  private static class ParDoStreamingTranslator<InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<InputT>, PCollectionTuple>> {

    @Override
    public void translateNode(
        PTransform<PCollection<InputT>, PCollectionTuple> transform,
        FlinkStreamingTranslationContext context) {

      DoFn<InputT, OutputT> doFn;
      try {
        doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      TupleTag<OutputT> mainOutputTag;
      try {
        mainOutputTag =
            (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      List<PCollectionView<?>> sideInputs;
      try {
        sideInputs = ParDoTranslation.getSideInputs(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      Map<String, PCollectionView<?>> sideInputMapping =
          ParDoTranslation.getSideInputMapping(context.getCurrentTransform());

      TupleTagList additionalOutputTags;
      try {
        additionalOutputTags =
            ParDoTranslation.getAdditionalOutputTags(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      DoFnSchemaInformation doFnSchemaInformation;
      doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.getCurrentTransform());

      ParDoTranslationHelper.translateParDo(
          getCurrentTransformName(context),
          doFn,
          context.getInput(transform),
          sideInputs,
          context.getOutputs(transform),
          mainOutputTag,
          additionalOutputTags.getAll(),
          doFnSchemaInformation,
          sideInputMapping,
          context,
          (doFn1,
              stepName,
              sideInputs1,
              mainOutputTag1,
              additionalOutputTags1,
              context1,
              windowingStrategy,
              tagsToOutputTags,
              tagsToCoders,
              tagsToIds,
              windowedInputCoder,
              outputCoders1,
              keyCoder,
              keySelector,
              transformedSideInputs,
              doFnSchemaInformation1,
              sideInputMapping1) ->
              new DoFnOperator<>(
                  doFn1,
                  stepName,
                  windowedInputCoder,
                  outputCoders1,
                  mainOutputTag1,
                  additionalOutputTags1,
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(
                      mainOutputTag1,
                      tagsToOutputTags,
                      tagsToCoders,
                      tagsToIds,
                      new SerializablePipelineOptions(context.getPipelineOptions())),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs1,
                  context1.getPipelineOptions(),
                  keyCoder,
                  keySelector,
                  doFnSchemaInformation1,
                  sideInputMapping1));
    }
  }

  private static class SplittableProcessElementsStreamingTranslator<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          SplittableParDoViaKeyedWorkItems.ProcessElements<
              InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>> {

    @Override
    public void translateNode(
        SplittableParDoViaKeyedWorkItems.ProcessElements<
                InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
            transform,
        FlinkStreamingTranslationContext context) {

      ParDoTranslationHelper.translateParDo(
          getCurrentTransformName(context),
          transform.newProcessFn(transform.getFn()),
          context.getInput(transform),
          transform.getSideInputs(),
          context.getOutputs(transform),
          transform.getMainOutputTag(),
          transform.getAdditionalOutputTags().getAll(),
          DoFnSchemaInformation.create(),
          Collections.emptyMap(),
          context,
          (doFn,
              stepName,
              sideInputs,
              mainOutputTag,
              additionalOutputTags,
              context1,
              windowingStrategy,
              tagsToOutputTags,
              tagsToCoders,
              tagsToIds,
              windowedInputCoder,
              outputCoders1,
              keyCoder,
              keySelector,
              transformedSideInputs,
              doFnSchemaInformation,
              sideInputMapping) ->
              new SplittableDoFnOperator<>(
                  doFn,
                  stepName,
                  windowedInputCoder,
                  outputCoders1,
                  mainOutputTag,
                  additionalOutputTags,
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(
                      mainOutputTag,
                      tagsToOutputTags,
                      tagsToCoders,
                      tagsToIds,
                      new SerializablePipelineOptions(context.getPipelineOptions())),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs,
                  context1.getPipelineOptions(),
                  keyCoder,
                  keySelector));
    }
  }

  private static class CreateViewStreamingTranslator<ElemT, ViewT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          CreateStreamingFlinkView.CreateFlinkPCollectionView<ElemT, ViewT>> {

    @Override
    public void translateNode(
        CreateStreamingFlinkView.CreateFlinkPCollectionView<ElemT, ViewT> transform,
        FlinkStreamingTranslationContext context) {
      // just forward
      DataStream<WindowedValue<List<ElemT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      PCollectionView<ViewT> view = transform.getView();

      context.setOutputDataStream(view, inputDataSet);
    }
  }

  private static class WindowAssignTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<T>, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {

      @SuppressWarnings("unchecked")
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>) context.getOutput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<T>> typeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DataStream<WindowedValue<T>> inputDataStream =
          context.getInputDataStream(context.getInput(transform));

      WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

      FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
          new FlinkAssignWindows<>(windowFn);

      String fullName = context.getOutput(transform).getName();
      SingleOutputStreamOperator<WindowedValue<T>> outputDataStream =
          inputDataStream
              .flatMap(assignWindowsFunction)
              .name(fullName)
              .uid(fullName)
              .returns(typeInfo);

      context.setOutputDataStream(context.getOutput(transform), outputDataStream);
    }
  }

  private static class ReshuffleTranslatorStreaming<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, InputT>>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {

      DataStream<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      context.setOutputDataStream(context.getOutput(transform), inputDataSet.rebalance());
    }
  }

  private static class GroupByKeyTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);
      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();
      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();
      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);
      String fullName = getCurrentTransformName(context);

      SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<InputT>>>> outDataStream;
      // Pre-aggregate before shuffle similar to group combine
      if (!context.isStreaming()) {
        outDataStream =
            FlinkStreamingAggregationsTranslators.batchGroupByKey(
                context,
                transform);
      } else {
        // No pre-aggregation in Streaming mode.
        KvToFlinkKeyKeySelector<K, InputT> keySelector =
            new KvToFlinkKeyKeySelector<>(inputKvCoder.getKeyCoder());

        Coder<WindowedValue<KV<K, Iterable<InputT>>>> outputCoder =
            WindowedValue.getFullCoder(
                KvCoder.of(
                    inputKvCoder.getKeyCoder(), IterableCoder.of(inputKvCoder.getValueCoder())),
                windowingStrategy.getWindowFn().windowCoder());

        TypeInformation<WindowedValue<KV<K, Iterable<InputT>>>> outputTypeInfo =
            new CoderTypeInformation<>(outputCoder, context.getPipelineOptions());

        WindowDoFnOperator<K, InputT, Iterable<InputT>> doFnOperator =
            FlinkStreamingAggregationsTranslators.getWindowedAggregateDoFnOperator(
                context,
                transform,
                inputKvCoder,
                outputCoder,
                SystemReduceFn.buffering(inputKvCoder.getValueCoder()),
                new HashMap<>(),
                Collections.emptyList());

        outDataStream =
            inputDataStream
                .keyBy(keySelector)
                .transform(fullName, outputTypeInfo, doFnOperator)
                .uid(fullName);
      }
      context.setOutputDataStream(context.getOutput(transform), outDataStream);
    }
  }

  private static class CombinePerKeyTranslator<K, InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

    @Override
    boolean canTranslate(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
        FlinkStreamingTranslationContext context) {
      // if we have a merging window strategy and side inputs we cannot
      // translate as a proper combine. We have to group and then run the combine
      // over the final grouped values.
      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      return !windowingStrategy.needsMerge()
          || ((Combine.PerKey) transform).getSideInputs().isEmpty();
    }

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
        FlinkStreamingTranslationContext context) {
      String fullName = getCurrentTransformName(context);

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();
      Coder<K> keyCoder = inputKvCoder.getKeyCoder();
      Coder<WindowedValue<KV<K, OutputT>>> outputCoder =
          context.getWindowedInputCoder(context.getOutput(transform));

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      @SuppressWarnings("unchecked")
      GlobalCombineFn<InputT, ?, OutputT> combineFn = ((Combine.PerKey) transform).getFn();

      TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      @SuppressWarnings("unchecked")
      List<PCollectionView<?>> sideInputs = ((Combine.PerKey) transform).getSideInputs();

      KeyedStream<WindowedValue<KV<K, InputT>>, FlinkKey> keyedStream =
          inputDataStream.keyBy(
              new KvToFlinkKeyKeySelector<>(keyCoder));

      if (sideInputs.isEmpty()) {
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream;

        if (!context.isStreaming()) {
          outDataStream =
              FlinkStreamingAggregationsTranslators.batchCombinePerKeyNoSideInputs(
                  context, transform, combineFn);
        } else {
          WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
              FlinkStreamingAggregationsTranslators.getWindowedAggregateDoFnOperator(
                  context,
                  transform,
                  inputKvCoder,
                  outputCoder,
                  combineFn,
                  new HashMap<>(),
                  Collections.emptyList());

          outDataStream =
              keyedStream.transform(fullName, outputTypeInfo, doFnOperator).uid(fullName);
        }

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformSideInputs =
            transformSideInputs(sideInputs, context);
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream;

        if (!context.isStreaming()) {
          outDataStream =
              FlinkStreamingAggregationsTranslators.batchCombinePerKey(
                  context, transform, combineFn, transformSideInputs.f0, sideInputs);
        } else {
          WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
              FlinkStreamingAggregationsTranslators.getWindowedAggregateDoFnOperator(
                  context,
                  transform,
                  inputKvCoder,
                  outputCoder,
                  combineFn,
                  transformSideInputs.f0,
                  sideInputs);

          outDataStream =
              FlinkStreamingAggregationsTranslators.buildTwoInputStream(
                  keyedStream,
                  transformSideInputs.f1,
                  transform.getName(),
                  doFnOperator,
                  outputTypeInfo);
        }

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      }
    }
  }

  private static class GBKIntoKeyedWorkItemsTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>>> {

    @Override
    boolean canTranslate(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {
      return true;
    }

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder =
          SingletonKeyedWorkItemCoder.of(
              inputKvCoder.getKeyCoder(),
              inputKvCoder.getValueCoder(),
              input.getWindowingStrategy().getWindowFn().windowCoder());

      WindowedValue.ValueOnlyWindowedValueCoder<KeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getValueOnlyCoder(workItemCoder);

      CoderTypeInformation<WindowedValue<KeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder, context.getPipelineOptions());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      DataStream<WindowedValue<KeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItemInGlobalWindow<>(context.getPipelineOptions()))
              .returns(workItemTypeInfo)
              .name("ToKeyedWorkItem");

      KeyedStream<WindowedValue<KeyedWorkItem<K, InputT>>, FlinkKey> keyedWorkItemStream =
          workItemStream.keyBy(
              new WorkItemKeySelector<>(inputKvCoder.getKeyCoder()));

      context.setOutputDataStream(context.getOutput(transform), keyedWorkItemStream);
    }
  }

  private static class ToKeyedWorkItemInGlobalWindow<K, InputT>
      extends RichFlatMapFunction<
          WindowedValue<KV<K, InputT>>, WindowedValue<KeyedWorkItem<K, InputT>>> {

    private final SerializablePipelineOptions options;

    ToKeyedWorkItemInGlobalWindow(PipelineOptions options) {
      this.options = new SerializablePipelineOptions(options);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public void flatMap(
        WindowedValue<KV<K, InputT>> inWithMultipleWindows,
        Collector<WindowedValue<KeyedWorkItem<K, InputT>>> out)
        throws Exception {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://github.com/apache/beam/issues/18358
      for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        SingletonKeyedWorkItem<K, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(), in.withValue(in.getValue().getValue()));

        out.collect(WindowedValue.valueInGlobalWindow(workItem));
      }
    }
  }

  private static class FlattenPCollectionTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<T>, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {
      Map<TupleTag<?>, PCollection<?>> allInputs = context.getInputs(transform);

      if (allInputs.isEmpty()) {

        // create an empty dummy source to satisfy downstream operations
        // we cannot create an empty source in Flink, therefore we have to
        // add the flatMap that simply never forwards the single element
        DataStreamSource<String> dummySource =
            context.getExecutionEnvironment().fromElements("dummy");

        DataStream<WindowedValue<T>> result =
            dummySource
                .<WindowedValue<T>>flatMap(
                    (s, collector) -> {
                      // never return anything
                    })
                .returns(
                    new CoderTypeInformation<>(
                        WindowedValue.getFullCoder(
                            (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE),
                        context.getPipelineOptions()));
        context.setOutputDataStream(context.getOutput(transform), result);

      } else {
        DataStream<T> result = null;

        // Determine DataStreams that we use as input several times. For those, we need to uniquify
        // input streams because Flink seems to swallow watermarks when we have a union of one and
        // the same stream.
        Map<DataStream<T>, Integer> duplicates = new HashMap<>();
        for (PValue input : allInputs.values()) {
          DataStream<T> current = context.getInputDataStream(input);
          Integer oldValue = duplicates.put(current, 1);
          if (oldValue != null) {
            duplicates.put(current, oldValue + 1);
          }
        }

        for (PValue input : allInputs.values()) {
          DataStream<T> current = context.getInputDataStream(input);

          final Integer timesRequired = duplicates.get(current);
          if (timesRequired > 1) {
            current =
                current.flatMap(
                    new FlatMapFunction<T, T>() {
                      private static final long serialVersionUID = 1L;

                      @Override
                      public void flatMap(T t, Collector<T> collector) throws Exception {
                        collector.collect(t);
                      }
                    });
          }
          result = (result == null) ? current : result.union(current);
        }

        context.setOutputDataStream(context.getOutput(transform), result);
      }
    }
  }

  /** Registers classes specialized to the Flink runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class FlinkTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              CreateStreamingFlinkView.CreateFlinkPCollectionView.class,
              new CreateStreamingFlinkViewPayloadTranslator())
          .build();
    }
  }

  /** A translator just to vend the URN. */
  private static class CreateStreamingFlinkViewPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
          CreateStreamingFlinkView.CreateFlinkPCollectionView<?, ?>> {

    private CreateStreamingFlinkViewPayloadTranslator() {}

    @Override
    public String getUrn() {
      return CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN;
    }
  }

  /** A translator to support {@link TestStream} with Flink. */
  private static class TestStreamTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<TestStream<T>> {

    @Override
    void translateNode(TestStream<T> testStream, FlinkStreamingTranslationContext context) {
      Coder<T> valueCoder = testStream.getValueCoder();

      // Coder for the Elements in the TestStream
      TestStream.TestStreamCoder<T> testStreamCoder = TestStream.TestStreamCoder.of(valueCoder);
      final byte[] payload;
      try {
        payload = CoderUtils.encodeToByteArray(testStreamCoder, testStream);
      } catch (CoderException e) {
        throw new RuntimeException("Could not encode TestStream.", e);
      }

      SerializableFunction<byte[], TestStream<T>> testStreamDecoder =
          bytes -> {
            try {
              return CoderUtils.decodeFromByteArray(
                  TestStream.TestStreamCoder.of(valueCoder), bytes);
            } catch (CoderException e) {
              throw new RuntimeException("Can't decode TestStream payload.", e);
            }
          };

      WindowedValue.FullWindowedValueCoder<T> elementCoder =
          WindowedValue.getFullCoder(valueCoder, GlobalWindow.Coder.INSTANCE);

      DataStreamSource<WindowedValue<T>> source =
          context
              .getExecutionEnvironment()
              .addSource(
                  new TestStreamSource<>(testStreamDecoder, payload),
                  new CoderTypeInformation<>(elementCoder, context.getPipelineOptions()));

      context.setOutputDataStream(context.getOutput(testStream), source);
    }
  }

  /**
   * Wrapper for {@link UnboundedSourceWrapper}, which simplifies output type, namely, removes
   * {@link ValueWithRecordId}.
   */
  static class UnboundedSourceWrapperNoValueWithRecordId<
          OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends RichParallelSourceFunction<WindowedValue<OutputT>>
      implements BeamStoppableFunction,
          CheckpointListener,
          CheckpointedFunction,
          ProcessingTimeCallback {

    private final UnboundedSourceWrapper<OutputT, CheckpointMarkT> unboundedSourceWrapper;

    @VisibleForTesting
    UnboundedSourceWrapper<OutputT, CheckpointMarkT> getUnderlyingSource() {
      return unboundedSourceWrapper;
    }

    UnboundedSourceWrapperNoValueWithRecordId(
        UnboundedSourceWrapper<OutputT, CheckpointMarkT> unboundedSourceWrapper) {
      this.unboundedSourceWrapper = unboundedSourceWrapper;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      unboundedSourceWrapper.setRuntimeContext(getRuntimeContext());
      unboundedSourceWrapper.open(parameters);
    }

    @Override
    public void run(SourceContext<WindowedValue<OutputT>> ctx) throws Exception {
      unboundedSourceWrapper.run(new SourceContextWrapper(ctx));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      unboundedSourceWrapper.initializeState(context);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      unboundedSourceWrapper.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      unboundedSourceWrapper.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void stop() {
      unboundedSourceWrapper.stop();
    }

    @Override
    public void cancel() {
      unboundedSourceWrapper.cancel();
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
      unboundedSourceWrapper.onProcessingTime(timestamp);
    }

    private final class SourceContextWrapper
        implements SourceContext<WindowedValue<ValueWithRecordId<OutputT>>> {

      private final SourceContext<WindowedValue<OutputT>> ctx;

      private SourceContextWrapper(SourceContext<WindowedValue<OutputT>> ctx) {
        this.ctx = ctx;
      }

      @Override
      public void collect(WindowedValue<ValueWithRecordId<OutputT>> element) {
        OutputT originalValue = element.getValue().getValue();
        WindowedValue<OutputT> output =
            WindowedValue.of(
                originalValue, element.getTimestamp(), element.getWindows(), element.getPane());
        ctx.collect(output);
      }

      @Override
      public void collectWithTimestamp(
          WindowedValue<ValueWithRecordId<OutputT>> element, long timestamp) {
        OutputT originalValue = element.getValue().getValue();
        WindowedValue<OutputT> output =
            WindowedValue.of(
                originalValue, element.getTimestamp(), element.getWindows(), element.getPane());
        ctx.collectWithTimestamp(output, timestamp);
      }

      @Override
      public void emitWatermark(Watermark mark) {
        ctx.emitWatermark(mark);
      }

      @Override
      public void markAsTemporarilyIdle() {
        ctx.markAsTemporarilyIdle();
      }

      @Override
      public Object getCheckpointLock() {
        return ctx.getCheckpointLock();
      }

      @Override
      public void close() {
        ctx.close();
      }
    }
  }
}
