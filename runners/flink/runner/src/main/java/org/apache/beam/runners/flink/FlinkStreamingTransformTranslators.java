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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.runners.core.ElementAndRestriction;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SplittableDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.BoundedSourceWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all the mappings between Beam and Flink
 * <b>streaming</b> transformations. The {@link FlinkStreamingPipelineTranslator}
 * traverses the Beam job and comes here to translate the encountered Beam transformations
 * into Flink one, based on the mapping available in this class.
 */
class FlinkStreamingTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<
      Class<? extends PTransform>,
      FlinkStreamingPipelineTranslator.StreamTransformTranslator> TRANSLATORS = new HashMap<>();

  // here you can find all the available translators.
  static {
    TRANSLATORS.put(Read.Bounded.class, new BoundedReadSourceTranslator());
    TRANSLATORS.put(Read.Unbounded.class, new UnboundedReadSourceTranslator());
    TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteBoundStreamingTranslator());

    TRANSLATORS.put(ParDo.MultiOutput.class, new ParDoStreamingTranslator());
    TRANSLATORS.put(
        SplittableParDo.ProcessElements.class, new SplittableProcessElementsStreamingTranslator());
    TRANSLATORS.put(
        SplittableParDo.GBKIntoKeyedWorkItems.class, new GBKIntoKeyedWorkItemsTranslator());


    TRANSLATORS.put(Window.Assign.class, new WindowAssignTranslator());
    TRANSLATORS.put(Flatten.PCollections.class, new FlattenPCollectionTranslator());
    TRANSLATORS.put(
        FlinkStreamingViewOverrides.CreateFlinkPCollectionView.class,
        new CreateViewStreamingTranslator());

    TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslatorStreaming());
    TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslator());
    TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslator());
  }

  public static FlinkStreamingPipelineTranslator.StreamTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform) {
    return TRANSLATORS.get(transform.getClass());
  }

  // --------------------------------------------------------------------------------------------
  //  Transformation Implementations
  // --------------------------------------------------------------------------------------------

  private static class TextIOWriteBoundStreamingTranslator
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<TextIO.Write.Bound> {

    private static final Logger LOG =
        LoggerFactory.getLogger(TextIOWriteBoundStreamingTranslator.class);

    @Override
    public void translateNode(
        TextIO.Write.Bound transform,
        FlinkStreamingTranslationContext context) {
      PValue input = context.getInput(transform);
      DataStream<WindowedValue<String>> inputDataStream = context.getInputDataStream(input);

      String filenamePrefix = transform.getFilenamePrefix();
      String filenameSuffix = transform.getFilenameSuffix();
      boolean needsValidation = transform.needsValidation();
      int numShards = transform.getNumShards();
      String shardNameTemplate = transform.getShardNameTemplate();

      // TODO: Implement these. We need Flink support for this.
      LOG.warn(
          "Translation of TextIO.Write.needsValidation not yet supported. Is: {}.",
          needsValidation);
      LOG.warn(
          "Translation of TextIO.Write.filenameSuffix not yet supported. Is: {}.",
          filenameSuffix);
      LOG.warn(
          "Translation of TextIO.Write.shardNameTemplate not yet supported. Is: {}.",
          shardNameTemplate);

      DataStream<String> dataSink = inputDataStream
          .flatMap(new FlatMapFunction<WindowedValue<String>, String>() {
            @Override
            public void flatMap(
                WindowedValue<String> value,
                Collector<String> out)
                throws Exception {
              out.collect(value.getValue());
            }
          });
      DataStreamSink<String> output =
          dataSink.writeAsText(filenamePrefix, FileSystem.WriteMode.OVERWRITE);

      if (numShards > 0) {
        output.setParallelism(numShards);
      }
    }
  }

  private static class UnboundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<Read.Unbounded<T>> {

    @Override
    public void translateNode(
        Read.Unbounded<T> transform,
        FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DataStream<WindowedValue<T>> source;
      try {
        UnboundedSourceWrapper<T, ?> sourceWrapper =
            new UnboundedSourceWrapper<>(
                context.getPipelineOptions(),
                transform.getSource(),
                context.getExecutionEnvironment().getParallelism());
        source = context
            .getExecutionEnvironment()
            .addSource(sourceWrapper).name(transform.getName()).returns(outputTypeInfo);
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while translating UnboundedSource: " + transform.getSource(), e);
      }

      context.setOutputDataStream(output, source);
    }
  }

  private static class BoundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<Read.Bounded<T>> {

    @Override
    public void translateNode(
        Read.Bounded<T> transform,
        FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));


      DataStream<WindowedValue<T>> source;
      try {
        BoundedSourceWrapper<T> sourceWrapper =
            new BoundedSourceWrapper<>(
                context.getPipelineOptions(),
                transform.getSource(),
                context.getExecutionEnvironment().getParallelism());
        source = context
            .getExecutionEnvironment()
            .addSource(sourceWrapper).name(transform.getName()).returns(outputTypeInfo);
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while translating BoundedSource: " + transform.getSource(), e);
      }

      context.setOutputDataStream(output, source);
    }
  }

  /**
   * Wraps each element in a {@link RawUnionValue} with the given tag id.
   */
  private static class ToRawUnion<T> implements MapFunction<T, RawUnionValue> {
    private final int intTag;

    public ToRawUnion(int intTag) {
      this.intTag = intTag;
    }

    @Override
    public RawUnionValue map(T o) throws Exception {
      return new RawUnionValue(intTag, o);
    }
  }

  private static Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>>
        transformSideInputs(
          Collection<PCollectionView<?>> sideInputs,
          FlinkStreamingTranslationContext context) {

    // collect all side inputs
    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    int count = 0;
    for (PCollectionView<?> sideInput: sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      intToViewMapping.put(count, sideInput);
      tagToIntMapping.put(tag, count);
      count++;
      Coder<Iterable<WindowedValue<?>>> coder = sideInput.getCoderInternal();
    }


    List<Coder<?>> inputCoders = new ArrayList<>();
    for (PCollectionView<?> sideInput: sideInputs) {
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException(
            "Input Stream TypeInformation is no CoderTypeInformation.");
      }

      Coder<?> coder = ((CoderTypeInformation) tpe).getCoder();
      inputCoders.add(coder);
    }

    UnionCoder unionCoder = UnionCoder.of(inputCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
        new CoderTypeInformation<>(unionCoder);

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (PCollectionView<?> sideInput: sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      DataStream<RawUnionValue> unionValueStream =
          sideInputStream.map(new ToRawUnion<>(intTag)).returns(unionTypeInformation);

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
   * Helper for translating {@link ParDo.MultiOutput} and {@link SplittableParDo.ProcessElements}.
   */
  static class ParDoTranslationHelper {

    interface DoFnOperatorFactory<InputT, OutputT> {
      DoFnOperator<InputT, OutputT, RawUnionValue> createDoFnOperator(
          DoFn<InputT, OutputT> doFn,
          List<PCollectionView<?>> sideInputs,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          FlinkStreamingTranslationContext context,
          WindowingStrategy<?, ?> windowingStrategy,
          Map<TupleTag<?>, Integer> tagsToLabels,
          Coder<WindowedValue<InputT>> inputCoder,
          Coder keyCoder,
          Map<Integer, PCollectionView<?>> transformedSideInputs);
    }

    static <InputT, OutputT> void translateParDo(
        String transformName,
        DoFn<InputT, OutputT> doFn,
        PCollection<InputT> input,
        List<PCollectionView<?>> sideInputs,
        Map<TupleTag<?>, PValue> outputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        FlinkStreamingTranslationContext context,
        DoFnOperatorFactory<InputT, OutputT> doFnOperatorFactory) {

      // we assume that the transformation does not change the windowing strategy.
      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

      Map<TupleTag<?>, Integer> tagsToLabels =
          transformTupleTagsToLabels(mainOutputTag, outputs);

      SingleOutputStreamOperator<RawUnionValue> unionOutputStream;

      Coder<WindowedValue<InputT>> inputCoder = context.getCoder(input);

      DataStream<WindowedValue<InputT>> inputDataStream = context.getInputDataStream(input);

      Coder keyCoder = null;
      boolean stateful = false;
      DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
      if (signature.stateDeclarations().size() > 0
          || signature.timerDeclarations().size() > 0) {
        // Based on the fact that the signature is stateful, DoFnSignatures ensures
        // that it is also keyed
        keyCoder = ((KvCoder) input.getCoder()).getKeyCoder();
        inputDataStream = inputDataStream.keyBy(new KvToByteBufferKeySelector(keyCoder));
        stateful = true;
      } else if (doFn instanceof SplittableParDo.ProcessFn) {
        // we know that it is keyed on String
        keyCoder = StringUtf8Coder.of();
        stateful = true;
      }

      if (sideInputs.isEmpty()) {
        DoFnOperator<InputT, OutputT, RawUnionValue> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToLabels,
                inputCoder,
                keyCoder,
                new HashMap<Integer, PCollectionView<?>>() /* side-input mapping */);

        UnionCoder outputUnionCoder = createUnionCoder(outputs);

        CoderTypeInformation<RawUnionValue> outputUnionTypeInformation =
            new CoderTypeInformation<>(outputUnionCoder);

        unionOutputStream = inputDataStream
            .transform(transformName, outputUnionTypeInformation, doFnOperator);

      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformedSideInputs =
            transformSideInputs(sideInputs, context);

        DoFnOperator<InputT, OutputT, RawUnionValue> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToLabels,
                inputCoder,
                keyCoder,
                transformedSideInputs.f0);

        UnionCoder outputUnionCoder = createUnionCoder(outputs);

        CoderTypeInformation<RawUnionValue> outputUnionTypeInformation =
            new CoderTypeInformation<>(outputUnionCoder);

        if (stateful) {
          // we have to manually contruct the two-input transform because we're not
          // allowed to have only one input keyed, normally.
          KeyedStream keyedStream = (KeyedStream<?, InputT>) inputDataStream;
          TwoInputTransformation<
              WindowedValue<KV<?, InputT>>,
              RawUnionValue,
              WindowedValue<OutputT>> rawFlinkTransform = new TwoInputTransformation(
              keyedStream.getTransformation(),
              transformedSideInputs.f1.broadcast().getTransformation(),
              transformName,
              (TwoInputStreamOperator) doFnOperator,
              outputUnionTypeInformation,
              keyedStream.getParallelism());

          rawFlinkTransform.setStateKeyType(keyedStream.getKeyType());
          rawFlinkTransform.setStateKeySelectors(keyedStream.getKeySelector(), null);

          unionOutputStream = new SingleOutputStreamOperator(
                  keyedStream.getExecutionEnvironment(),
                  rawFlinkTransform) {}; // we have to cheat around the ctor being protected

          keyedStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

        } else {
          unionOutputStream = inputDataStream
              .connect(transformedSideInputs.f1.broadcast())
              .transform(transformName, outputUnionTypeInformation, doFnOperator);
        }
      }

      SplitStream<RawUnionValue> splitStream = unionOutputStream
              .split(new OutputSelector<RawUnionValue>() {
                @Override
                public Iterable<String> select(RawUnionValue value) {
                  return Collections.singletonList(Integer.toString(value.getUnionTag()));
                }
              });

      for (Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
        final int outputTag = tagsToLabels.get(output.getKey());

        TypeInformation outputTypeInfo = context.getTypeInfo((PCollection<?>) output.getValue());

        @SuppressWarnings("unchecked")
        DataStream unwrapped = splitStream.select(String.valueOf(outputTag))
          .flatMap(new FlatMapFunction<RawUnionValue, Object>() {
            @Override
            public void flatMap(RawUnionValue value, Collector<Object> out) throws Exception {
              out.collect(value.getValue());
            }
          }).returns(outputTypeInfo);

        context.setOutputDataStream(output.getValue(), unwrapped);
      }
    }

    private static Map<TupleTag<?>, Integer> transformTupleTagsToLabels(
        TupleTag<?> mainTag,
        Map<TupleTag<?>, PValue> allTaggedValues) {

      Map<TupleTag<?>, Integer> tagToLabelMap = Maps.newHashMap();
      int count = 0;
      tagToLabelMap.put(mainTag, count++);
      for (TupleTag<?> key : allTaggedValues.keySet()) {
        if (!tagToLabelMap.containsKey(key)) {
          tagToLabelMap.put(key, count++);
        }
      }
      return tagToLabelMap;
    }

    private static UnionCoder createUnionCoder(Map<TupleTag<?>, PValue> taggedCollections) {
      List<Coder<?>> outputCoders = Lists.newArrayList();
      for (PValue taggedColl : taggedCollections.values()) {
        checkArgument(
            taggedColl instanceof PCollection,
            "A Union Coder can only be created for a Collection of Tagged %s. Got %s",
            PCollection.class.getSimpleName(),
            taggedColl.getClass().getSimpleName());
        PCollection<?> coll = (PCollection<?>) taggedColl;
        WindowedValue.FullWindowedValueCoder<?> windowedValueCoder =
            WindowedValue.getFullCoder(
                coll.getCoder(),
                coll.getWindowingStrategy().getWindowFn().windowCoder());
        outputCoders.add(windowedValueCoder);
      }
      return UnionCoder.of(outputCoders);
    }
  }

  private static class ParDoStreamingTranslator<InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      ParDo.MultiOutput<InputT, OutputT>> {

    @Override
    public void translateNode(
        ParDo.MultiOutput<InputT, OutputT> transform,
        FlinkStreamingTranslationContext context) {

      ParDoTranslationHelper.translateParDo(
          transform.getName(),
          transform.getFn(),
          (PCollection<InputT>) context.getInput(transform),
          transform.getSideInputs(),
          context.getOutputs(transform),
          transform.getMainOutputTag(),
          transform.getAdditionalOutputTags().getAll(),
          context,
          new ParDoTranslationHelper.DoFnOperatorFactory<InputT, OutputT>() {
            @Override
            public DoFnOperator<InputT, OutputT, RawUnionValue> createDoFnOperator(
                DoFn<InputT, OutputT> doFn,
                List<PCollectionView<?>> sideInputs,
                TupleTag<OutputT> mainOutputTag,
                List<TupleTag<?>> additionalOutputTags,
                FlinkStreamingTranslationContext context,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, Integer> tagsToLabels,
                Coder<WindowedValue<InputT>> inputCoder,
                Coder keyCoder,
                Map<Integer, PCollectionView<?>> transformedSideInputs) {
              return new DoFnOperator<>(
                  doFn,
                  inputCoder,
                  mainOutputTag,
                  additionalOutputTags,
                  new DoFnOperator.MultiOutputOutputManagerFactory(tagsToLabels),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs,
                  context.getPipelineOptions(),
                  keyCoder);
            }
          });
    }
  }

  private static class SplittableProcessElementsStreamingTranslator<
      InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT, TrackerT>> {

    @Override
    public void translateNode(
        SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT, TrackerT> transform,
        FlinkStreamingTranslationContext context) {

      ParDoTranslationHelper.translateParDo(
          transform.getName(),
          transform.newProcessFn(transform.getFn()),
          (PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>)
              context.getInput(transform),
          transform.getSideInputs(),
          context.getOutputs(transform),
          transform.getMainOutputTag(),
          transform.getAdditionalOutputTags().getAll(),
          context,
          new ParDoTranslationHelper.DoFnOperatorFactory<
              KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>() {
            @Override
            public DoFnOperator<
                KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>,
                OutputT,
                RawUnionValue> createDoFnOperator(
                    DoFn<
                        KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>,
                        OutputT> doFn,
                    List<PCollectionView<?>> sideInputs,
                    TupleTag<OutputT> mainOutputTag,
                    List<TupleTag<?>> additionalOutputTags,
                    FlinkStreamingTranslationContext context,
                    WindowingStrategy<?, ?> windowingStrategy,
                    Map<TupleTag<?>, Integer> tagsToLabels,
                    Coder<
                        WindowedValue<
                            KeyedWorkItem<
                                String,
                                ElementAndRestriction<InputT, RestrictionT>>>> inputCoder,
                    Coder keyCoder,
                    Map<Integer, PCollectionView<?>> transformedSideInputs) {
              return new SplittableDoFnOperator<>(
                  doFn,
                  inputCoder,
                  mainOutputTag,
                  additionalOutputTags,
                  new DoFnOperator.MultiOutputOutputManagerFactory(tagsToLabels),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs,
                  context.getPipelineOptions(),
                  keyCoder);
            }
          });
    }
  }

  private static class CreateViewStreamingTranslator<ElemT, ViewT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      FlinkStreamingViewOverrides.CreateFlinkPCollectionView<ElemT, ViewT>> {

    @Override
    public void translateNode(
        FlinkStreamingViewOverrides.CreateFlinkPCollectionView<ElemT, ViewT> transform,
        FlinkStreamingTranslationContext context) {
      // just forward
      DataStream<WindowedValue<List<ElemT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      PCollectionView<ViewT> view = context.getOutput(transform);

      context.setOutputDataStream(view, inputDataSet);
    }
  }

  private static class WindowAssignTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<Window.Assign<T>> {

    @Override
    public void translateNode(
        Window.Assign<T> transform,
        FlinkStreamingTranslationContext context) {

      @SuppressWarnings("unchecked")
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>)
              context.getOutput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<T>> typeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DataStream<WindowedValue<T>> inputDataStream =
          context.getInputDataStream(context.getInput(transform));

      WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

      FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
          new FlinkAssignWindows<>(windowFn);

      SingleOutputStreamOperator<WindowedValue<T>> outputDataStream = inputDataStream
          .flatMap(assignWindowsFunction)
          .name(context.getOutput(transform).getName())
          .returns(typeInfo);

      context.setOutputDataStream(context.getOutput(transform), outputDataStream);
    }
  }

  private static class ReshuffleTranslatorStreaming<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<Reshuffle<K, InputT>> {

    @Override
    public void translateNode(
        Reshuffle<K, InputT> transform,
        FlinkStreamingTranslationContext context) {

      DataStream<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      context.setOutputDataStream(context.getOutput(transform), inputDataSet.rebalance());

    }
  }


  private static class GroupByKeyTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<GroupByKey<K, InputT>> {

    @Override
    public void translateNode(
        GroupByKey<K, InputT> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getFullCoder(
              workItemCoder,
              input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
          WindowedValue<
              SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, BoundedWindow> reduceFn =
          SystemReduceFn.buffering(inputKvCoder.getValueCoder());

      TypeInformation<WindowedValue<KV<K, Iterable<InputT>>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DoFnOperator.DefaultOutputManagerFactory<
            WindowedValue<KV<K, Iterable<InputT>>>> outputManagerFactory =
          new DoFnOperator.DefaultOutputManagerFactory<>();

      WindowDoFnOperator<K, InputT, Iterable<InputT>> doFnOperator =
          new WindowDoFnOperator<>(
              reduceFn,
              (Coder) windowedWorkItemCoder,
              new TupleTag<KV<K, Iterable<InputT>>>("main output"),
              Collections.<TupleTag<?>>emptyList(),
              outputManagerFactory,
              windowingStrategy,
              new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
              Collections.<PCollectionView<?>>emptyList(), /* side inputs */
              context.getPipelineOptions(),
              inputKvCoder.getKeyCoder());

      // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
      // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
      @SuppressWarnings("unchecked")
      SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<InputT>>>> outDataStream =
          keyedWorkItemStream
              .transform(
                  transform.getName(),
                  outputTypeInfo,
                  (OneInputStreamOperator) doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);

    }
  }

  private static class CombinePerKeyTranslator<K, InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      Combine.PerKey<K, InputT, OutputT>> {

    @Override
    boolean canTranslate(
        Combine.PerKey<K, InputT, OutputT> transform,
        FlinkStreamingTranslationContext context) {

      // if we have a merging window strategy and side inputs we cannot
      // translate as a proper combine. We have to group and then run the combine
      // over the final grouped values.
      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      return windowingStrategy.getWindowFn().isNonMerging() || transform.getSideInputs().isEmpty();
    }

    @Override
    public void translateNode(
        Combine.PerKey<K, InputT, OutputT> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
            WindowedValue.getFullCoder(
                workItemCoder,
                input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
            WindowedValue<
                SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn = SystemReduceFn.combining(
          inputKvCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(
              transform.getFn(), input.getPipeline().getCoderRegistry(), inputKvCoder));

      TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      List<PCollectionView<?>> sideInputs = transform.getSideInputs();

      if (sideInputs.isEmpty()) {

        WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
            new WindowDoFnOperator<>(
                reduceFn,
                (Coder) windowedWorkItemCoder,
                new TupleTag<KV<K, OutputT>>("main output"),
                Collections.<TupleTag<?>>emptyList(),
                new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<KV<K, OutputT>>>(),
                windowingStrategy,
                new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
                Collections.<PCollectionView<?>>emptyList(), /* side inputs */
                context.getPipelineOptions(),
                inputKvCoder.getKeyCoder());

        // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
        // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
        @SuppressWarnings("unchecked")
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
            keyedWorkItemStream.transform(
                transform.getName(), outputTypeInfo, (OneInputStreamOperator) doFnOperator);

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformSideInputs =
            transformSideInputs(sideInputs, context);

        WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
            new WindowDoFnOperator<>(
                reduceFn,
                (Coder) windowedWorkItemCoder,
                new TupleTag<KV<K, OutputT>>("main output"),
                Collections.<TupleTag<?>>emptyList(),
                new DoFnOperator.DefaultOutputManagerFactory<WindowedValue<KV<K, OutputT>>>(),
                windowingStrategy,
                transformSideInputs.f0,
                sideInputs,
                context.getPipelineOptions(),
                inputKvCoder.getKeyCoder());

        // we have to manually contruct the two-input transform because we're not
        // allowed to have only one input keyed, normally.

        TwoInputTransformation<
            WindowedValue<SingletonKeyedWorkItem<K, InputT>>,
            RawUnionValue,
            WindowedValue<KV<K, OutputT>>> rawFlinkTransform = new TwoInputTransformation<>(
            keyedWorkItemStream.getTransformation(),
            transformSideInputs.f1.broadcast().getTransformation(),
            transform.getName(),
            (TwoInputStreamOperator) doFnOperator,
            outputTypeInfo,
            keyedWorkItemStream.getParallelism());

        rawFlinkTransform.setStateKeyType(keyedWorkItemStream.getKeyType());
        rawFlinkTransform.setStateKeySelectors(keyedWorkItemStream.getKeySelector(), null);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
            new SingleOutputStreamOperator(
                keyedWorkItemStream.getExecutionEnvironment(),
                rawFlinkTransform) {}; // we have to cheat around the ctor being protected

        keyedWorkItemStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      }
    }
  }

  private static class GBKIntoKeyedWorkItemsTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      SplittableParDo.GBKIntoKeyedWorkItems<K, InputT>> {

    @Override
    boolean canTranslate(
        SplittableParDo.GBKIntoKeyedWorkItems<K, InputT> transform,
        FlinkStreamingTranslationContext context) {
      return true;
    }

    @Override
    public void translateNode(
        SplittableParDo.GBKIntoKeyedWorkItems<K, InputT> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());


      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getFullCoder(
              workItemCoder,
              input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
          WindowedValue<
              SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      context.setOutputDataStream(context.getOutput(transform), keyedWorkItemStream);
    }
  }

  private static class FlattenPCollectionTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      Flatten.PCollections<T>> {

    @Override
    public void translateNode(
        Flatten.PCollections<T> transform,
        FlinkStreamingTranslationContext context) {
      Map<TupleTag<?>, PValue> allInputs = context.getInputs(transform);

      if (allInputs.isEmpty()) {

        // create an empty dummy source to satisfy downstream operations
        // we cannot create an empty source in Flink, therefore we have to
        // add the flatMap that simply never forwards the single element
        DataStreamSource<String> dummySource =
            context.getExecutionEnvironment().fromElements("dummy");

        DataStream<WindowedValue<T>> result = dummySource.flatMap(
            new FlatMapFunction<String, WindowedValue<T>>() {
              @Override
              public void flatMap(
                  String s,
                  Collector<WindowedValue<T>> collector) throws Exception {
                // never return anything
              }
            }).returns(
            new CoderTypeInformation<>(
                WindowedValue.getFullCoder(
                    (Coder<T>) VoidCoder.of(),
                    GlobalWindow.Coder.INSTANCE)));
        context.setOutputDataStream(context.getOutput(transform), result);

      } else {
        DataStream<T> result = null;
        for (PValue input : allInputs.values()) {
          DataStream<T> current = context.getInputDataStream(input);
          result = (result == null) ? current : result.union(current);
        }
        context.setOutputDataStream(context.getOutput(transform), result);
      }
    }
  }

  private static class ToKeyedWorkItem<K, InputT>
      extends RichFlatMapFunction<
      WindowedValue<KV<K, InputT>>,
      WindowedValue<SingletonKeyedWorkItem<K, InputT>>> {

    @Override
    public void flatMap(
        WindowedValue<KV<K, InputT>> inWithMultipleWindows,
        Collector<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> out) throws Exception {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://issues.apache.org/jira/browse/BEAM-1850
      for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        SingletonKeyedWorkItem<K, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(),
                in.withValue(in.getValue().getValue()));

        out.collect(in.withValue(workItem));
      }
    }
  }

}
