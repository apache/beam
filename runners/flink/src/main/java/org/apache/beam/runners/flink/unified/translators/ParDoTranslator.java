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
package org.apache.beam.runners.flink.unified.translators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.runners.flink.unified.translators.functions.ToRawUnion;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.OutputTag;

public class ParDoTranslator<InputT, OutputT>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  /**
   * Helper for translating {@code ParDo.MultiOutput} and {@link
   * SplittableParDoViaKeyedWorkItems.ProcessElements}.
   */
  static class ParDoTranslationHelper {

    interface DoFnOperatorFactory<InputT, OutputT> {
      DoFnOperator<InputT, OutputT> createDoFnOperator(
          DoFn<InputT, OutputT> doFn,
          String stepName,
          List<PCollectionView<?>> sideInputs,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          UnifiedTranslationContext context,
          WindowingStrategy<?, ?> windowingStrategy,
          Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
          Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
          Map<TupleTag<?>, Integer> tagsToIds,
          Coder<WindowedValue<InputT>> windowedInputCoder,
          Map<TupleTag<?>, Coder<?>> outputCoders,
          Coder<?> keyCoder,
          KeySelector<WindowedValue<InputT>, ?> keySelector,
          Map<Integer, PCollectionView<?>> transformedSideInputs,
          DoFnSchemaInformation doFnSchemaInformation,
          Map<String, PCollectionView<?>> sideInputMapping);
    }

    private static String getMainInput(
        Map<String, String> inputsMap, List<PCollectionView<?>> sideInputs) {
      Set<String> sideInputTags =
          sideInputs.stream().map(s -> s.getTagInternal().getId()).collect(Collectors.toSet());

      List<Map.Entry<String, String>> ins =
          inputsMap.entrySet().stream()
              .filter(i -> !sideInputTags.contains(i.getKey()))
              .collect(Collectors.toList());

      return Iterables.getOnlyElement(ins).getValue();
    }

    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "nullness" // TODO(https://github.com/apache/beam/issues/20497)
    })
    static <InputT, OutputT> void translateParDo(
        Pipeline pipeline,
        PTransformNode transform,
        DoFn<InputT, OutputT> doFn,
        List<PCollectionView<?>> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<String, PCollectionView<?>> sideInputMapping,
        UnifiedTranslationContext context,
        DoFnOperatorFactory<InputT, OutputT> doFnOperatorFactory) {

      RunnerApi.PTransform pTransform = transform.getTransform();
      String inputPCollectionId = getMainInput(pTransform.getInputsMap(), sideInputs);

      String transformName = pTransform.getUniqueName();

      // we assume that the transformation does not change the windowing strategy.
      WindowingStrategy<?, ?> windowingStrategy =
          context.getWindowingStrategy(pipeline, inputPCollectionId);

      Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newHashMap();
      SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;

      Coder<WindowedValue<InputT>> windowedInputCoder =
          context.getWindowedInputCoder(pipeline, inputPCollectionId);

      // TupleTag to outputs PCollection IDs
      Map<TupleTag<?>, String> outputs =
          pTransform.getOutputsMap().entrySet().stream()
              .collect(Collectors.toMap(x -> new TupleTag<>(x.getKey()), Map.Entry::getValue));

      Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders =
          outputs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      x -> (Coder) context.getWindowedInputCoder(pipeline, x.getValue())));

      // TODO: use this if output is a Row ???
      Map<TupleTag<?>, Coder<?>> outputCoders =
          outputs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      x -> (Coder) context.getOutputCoderHack(pipeline, x.getValue())));

      Map<String, String> sortedOutputs =
          outputs.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      x -> x.getKey().getId(), Map.Entry::getValue, (x, y) -> x, TreeMap::new));

      // We associate output tags with ids, the Integer is easier to serialize than TupleTag.
      Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
      int idCount = 0;
      tagsToIds.put(mainOutputTag, idCount++);
      for (Map.Entry<String, String> entry : sortedOutputs.entrySet()) {
        if (!tagsToOutputTags.containsKey(new TupleTag<>(entry.getKey()))) {
          tagsToOutputTags.put(
              new TupleTag<>(entry.getKey()),
              new OutputTag<WindowedValue<?>>(
                  entry.getKey(),
                  (TypeInformation) context.getTypeInfo(pipeline, entry.getValue())));
          tagsToIds.put(new TupleTag<>(entry.getKey()), idCount++);
        }
      }

      DataStream<WindowedValue<InputT>> inputDataStream =
          context.getDataStreamOrThrow(inputPCollectionId);

      Coder<?> keyCoder = null;
      KeySelector<WindowedValue<InputT>, ByteBuffer> keySelector = null;
      boolean stateful = false;
      DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
      if (!signature.stateDeclarations().isEmpty()
          || !signature.timerDeclarations().isEmpty()
          || !signature.timerFamilyDeclarations().isEmpty()) {
        // Based on the fact that the signature is stateful, DoFnSignatures ensures
        // that it is also keyed
        Coder<KV<?, ?>> inputKvCoder =
            ((WindowedValueCoder<KV<?, ?>>) windowedInputCoder).getValueCoder();
        keyCoder = ((KvCoder) inputKvCoder).getKeyCoder();
        keySelector =
            new KvToByteBufferKeySelector(
                keyCoder, new SerializablePipelineOptions(context.getPipelineOptions()));

        final PipelineNode.PTransformNode producer = context.getProducer(inputPCollectionId);
        final String previousUrn =
            producer != null
                ? PTransformTranslation.urnForTransformOrNull(producer.getTransform())
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
            new WorkItemKeySelector(
                keyCoder, new SerializablePipelineOptions(context.getPipelineOptions()));
        stateful = true;
      }

      TypeInformation<WindowedValue<OutputT>> outputTypeInformation =
          context.<OutputT>getTypeInfo(pipeline, outputs.get(mainOutputTag));

      if (sideInputs.isEmpty()) {
        DoFnOperator<InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                transformName,
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

        DoFnOperator<InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                transformName,
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
      context.addDataStream(outputs.get(mainOutputTag), outputStream);

      for (Map.Entry<TupleTag<?>, String> entry : outputs.entrySet()) {
        if (!entry.getKey().equals(mainOutputTag)) {
          context.addDataStream(
              entry.getValue(), outputStream.getSideOutput(tagsToOutputTags.get(entry.getKey())));
        }
      }
    }
  }

  private static Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>>
      transformSideInputs(
          Collection<PCollectionView<?>> sideInputs, UnifiedTranslationContext context) {

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
      DataStream<Object> sideInputStream = (DataStream) context.getSideInputDataStream(sideInput);
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
      Integer integerTag = tagToIntMapping.get(tag);
      if (integerTag == null) {
        throw new IllegalStateException("Tag to mapping should never return null");
      }
      final int intTag = integerTag;
      DataStream<Object> sideInputStream = (DataStream) context.getSideInputDataStream(sideInput);
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

  @Override
  public void translate(
      PTransformNode transform, Pipeline pipeline, UnifiedTranslationContext context) {

    ParDoPayload parDoPayload;
    try {
      parDoPayload = ParDoPayload.parseFrom(transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    DoFn<InputT, OutputT> doFn;
    try {
      doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(parDoPayload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    TupleTag<OutputT> mainOutputTag;
    try {
      mainOutputTag = (TupleTag<OutputT>) ParDoTranslation.getMainOutputTag(parDoPayload);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(parDoPayload);

    List<PCollectionView<?>> sideInputs = ImmutableList.copyOf(sideInputMapping.values());

    TupleTagList additionalOutputTags;
    try {
      additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(transform.getTransform());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(parDoPayload);

    ParDoTranslationHelper.translateParDo(
        pipeline,
        transform,
        doFn,
        sideInputs,
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
