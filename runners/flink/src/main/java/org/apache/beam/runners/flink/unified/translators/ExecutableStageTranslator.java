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

import static org.apache.beam.runners.core.construction.ExecutableStageTranslation.generateNameFromStagePayload;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.createOutputMap;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getWindowingStrategy;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.RunnerPCollectionView;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SdfByteBufferKeySelector;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator;
import org.apache.beam.runners.flink.unified.FlinkUnifiedPipelineTranslator.UnifiedTranslationContext;
import org.apache.beam.runners.flink.unified.translators.functions.ToRawUnion;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.OutputTag;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
  "nullness"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class ExecutableStageTranslator<InputT, OutputT>
    implements FlinkUnifiedPipelineTranslator.PTransformTranslator<
        FlinkUnifiedPipelineTranslator.UnifiedTranslationContext> {

  private static LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
      getSideInputIdToPCollectionViewMap(
          RunnerApi.ExecutableStagePayload stagePayload, RunnerApi.Components components) {

    RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(components);

    LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputs =
        new LinkedHashMap<>();
    // for PCollectionView compatibility, not used to transform materialization
    ViewFn<Iterable<WindowedValue<?>>, ?> viewFn =
        (ViewFn)
            new PCollectionViews.MultimapViewFn<>(
                (PCollectionViews.TypeDescriptorSupplier<Iterable<WindowedValue<Void>>>)
                    () -> TypeDescriptors.iterables(new TypeDescriptor<WindowedValue<Void>>() {}),
                (PCollectionViews.TypeDescriptorSupplier<Void>) TypeDescriptors::voids);

    for (RunnerApi.ExecutableStagePayload.SideInputId sideInputId :
        stagePayload.getSideInputsList()) {

      // TODO: local name is unique as long as only one transform with side input can be within a
      // stage
      String sideInputTag = sideInputId.getLocalName();
      String collectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      RunnerApi.WindowingStrategy windowingStrategyProto =
          components.getWindowingStrategiesOrThrow(
              components.getPcollectionsOrThrow(collectionId).getWindowingStrategyId());

      final WindowingStrategy<?, ?> windowingStrategy;
      try {
        windowingStrategy =
            WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(
            String.format(
                "Unable to hydrate side input windowing strategy %s.", windowingStrategyProto),
            e);
      }

      // TODO: Should use context.getWindowedInputCoder ?
      Coder<WindowedValue<Object>> coder0 = instantiateCoder(collectionId, components);

      // side input materialization via GBK (T -> Iterable<T>)
      WindowedValueCoder<Object> wvCoder = (WindowedValueCoder<Object>) coder0;
      Coder<WindowedValue<Iterable<Object>>> coder =
          wvCoder.withValueCoder(IterableCoder.of(wvCoder.getValueCoder()));

      sideInputs.put(
          sideInputId,
          new RunnerPCollectionView<>(
              null,
              new TupleTag<>(sideInputTag),
              viewFn,
              // TODO: support custom mapping fn
              windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
              windowingStrategy,
              coder));
    }
    return sideInputs;
  }

  private static class ToVoidKeyValue<T>
      extends RichMapFunction<WindowedValue<T>, WindowedValue<KV<Void, T>>> {

    private final SerializablePipelineOptions options;

    public ToVoidKeyValue(PipelineOptions pipelineOptions) {
      this.options = new SerializablePipelineOptions(pipelineOptions);
    }

    @Override
    public void open(Configuration parameters) {
      // Initialize FileSystems for any coders which may want to use the FileSystem,
      // see https://issues.apache.org/jira/browse/BEAM-8303
      FileSystems.setDefaultPipelineOptions(options.get());
    }

    @Override
    public WindowedValue<KV<Void, T>> map(WindowedValue<T> value) {
      return value.withValue(KV.of(null, value.getValue()));
    }
  }

  private TransformedSideInputs transformSideInputs(
      RunnerApi.ExecutableStagePayload stagePayload,
      RunnerApi.Components components,
      FlinkUnifiedPipelineTranslator.UnifiedTranslationContext context) {

    LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputs =
        getSideInputIdToPCollectionViewMap(stagePayload, components);

    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    List<WindowedValueCoder<KV<Void, Object>>> kvCoders = new ArrayList<>();
    List<Coder<?>> viewCoders = new ArrayList<>();

    int count = 0;
    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput :
        sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      intToViewMapping.put(count, sideInput.getValue());
      tagToIntMapping.put(tag, count);
      count++;
      String collectionId =
          components
              .getTransformsOrThrow(sideInput.getKey().getTransformId())
              .getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<Object> sideInputStream = context.getDataStreamOrThrow(collectionId);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException("Input Stream TypeInformation is no CoderTypeInformation.");
      }

      WindowedValueCoder<Object> coder =
          (WindowedValueCoder) ((CoderTypeInformation) tpe).getCoder();
      Coder<KV<Void, Object>> kvCoder = KvCoder.of(VoidCoder.of(), coder.getValueCoder());
      kvCoders.add(coder.withValueCoder(kvCoder));
      // coder for materialized view matching GBK below
      WindowedValueCoder<KV<Void, Iterable<Object>>> viewCoder =
          coder.withValueCoder(KvCoder.of(VoidCoder.of(), IterableCoder.of(coder.getValueCoder())));
      viewCoders.add(viewCoder);
    }

    // second pass, now that we gathered the input coders
    UnionCoder unionCoder = UnionCoder.of(viewCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
        new CoderTypeInformation<>(unionCoder, context.getPipelineOptions());

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput :
        sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      RunnerApi.PTransform pTransform =
          components.getTransformsOrThrow(sideInput.getKey().getTransformId());
      String collectionId = pTransform.getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<WindowedValue<?>> sideInputStream = context.getDataStreamOrThrow(collectionId);

      // insert GBK to materialize side input view
      String viewName =
          sideInput.getKey().getTransformId() + "-" + sideInput.getKey().getLocalName();
      WindowedValueCoder<KV<Void, Object>> kvCoder = kvCoders.get(intTag);

      DataStream<WindowedValue<KV<Void, Object>>> keyedSideInputStream =
          sideInputStream.map(new ToVoidKeyValue(context.getPipelineOptions()));

      SingleOutputStreamOperator<WindowedValue<KV<Void, Iterable<Object>>>> viewStream =
          GroupByKeyTranslator.addGBK(
              keyedSideInputStream,
              sideInput.getValue().getWindowingStrategyInternal(),
              kvCoder,
              viewName,
              context);

      // Assign a unique but consistent id to re-map operator state
      viewStream.uid(pTransform.getUniqueName() + "-" + sideInput.getKey().getLocalName());

      DataStream<RawUnionValue> unionValueStream =
          viewStream
              .map(new ToRawUnion<>(intTag, context.getPipelineOptions()))
              .returns(unionTypeInformation);

      if (sideInputUnion == null) {
        sideInputUnion = unionValueStream;
      } else {
        sideInputUnion = sideInputUnion.union(unionValueStream);
      }
    }

    return new TransformedSideInputs(intToViewMapping, sideInputUnion);
  }

  private static class TransformedSideInputs {
    final Map<Integer, PCollectionView<?>> unionTagToView;
    final DataStream<RawUnionValue> unionedSideInputs;

    TransformedSideInputs(
        Map<Integer, PCollectionView<?>> unionTagToView,
        DataStream<RawUnionValue> unionedSideInputs) {
      this.unionTagToView = unionTagToView;
      this.unionedSideInputs = unionedSideInputs;
    }
  }

  @Override
  public void translate(
      PipelineNode.PTransformNode transformNode,
      RunnerApi.Pipeline pipeline,
      UnifiedTranslationContext context) {

    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = transformNode.getTransform();
    Map<String, String> outputs = transform.getOutputsMap();

    final RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId = stagePayload.getInput();
    final TransformedSideInputs transformedSideInputs;

    if (stagePayload.getSideInputsCount() > 0) {
      transformedSideInputs = transformSideInputs(stagePayload, components, context);
    } else {
      transformedSideInputs = new TransformedSideInputs(Collections.emptyMap(), null);
    }

    Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newLinkedHashMap();
    Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newLinkedHashMap();

    // TODO: does it matter which output we designate as "main"
    final TupleTag<OutputT> mainOutputTag =
        outputs.isEmpty() ? null : new TupleTag<>(outputs.keySet().iterator().next());

    // associate output tags with ids, output manager uses these Integer ids to serialize state
    BiMap<String, Integer> outputIndexMap = createOutputMap(outputs.keySet());
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
    Map<String, TupleTag<?>> collectionIdToTupleTag = Maps.newHashMap();
    // order output names for deterministic mapping
    for (String localOutputName : new TreeMap<>(outputIndexMap).keySet()) {
      String collectionId = outputs.get(localOutputName);
      Coder<WindowedValue<?>> windowCoder = (Coder) instantiateCoder(collectionId, components);
      outputCoders.put(localOutputName, windowCoder);
      TupleTag<?> tupleTag = new TupleTag<>(localOutputName);
      CoderTypeInformation<WindowedValue<?>> typeInformation =
          new CoderTypeInformation<>(windowCoder, context.getPipelineOptions());
      tagsToOutputTags.put(tupleTag, new OutputTag<>(localOutputName, typeInformation));
      tagsToCoders.put(tupleTag, windowCoder);
      tagsToIds.put(tupleTag, outputIndexMap.get(localOutputName));
      collectionIdToTupleTag.put(collectionId, tupleTag);
    }

    final SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;
    DataStream<WindowedValue<InputT>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation =
        !outputs.isEmpty()
            ? new CoderTypeInformation(
                outputCoders.get(mainOutputTag.getId()), context.getPipelineOptions())
            : null;

    ArrayList<TupleTag<?>> additionalOutputTags = Lists.newArrayList();
    for (TupleTag<?> tupleTag : tagsToCoders.keySet()) {
      if (!mainOutputTag.getId().equals(tupleTag.getId())) {
        additionalOutputTags.add(tupleTag);
      }
    }

    final Coder<WindowedValue<InputT>> windowedInputCoder =
        instantiateCoder(inputPCollectionId, components);

    final boolean stateful =
        stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0;

    final boolean hasSdfProcessFn =
        stagePayload.getComponents().getTransformsMap().values().stream()
            .anyMatch(
                pTransform ->
                    pTransform
                        .getSpec()
                        .getUrn()
                        .equals(
                            PTransformTranslation
                                .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN));
    Coder keyCoder = null;
    KeySelector<WindowedValue<InputT>, ?> keySelector = null;
    if (stateful || hasSdfProcessFn) {
      // Stateful/SDF stages are only allowed of KV input.
      Coder valueCoder =
          ((WindowedValue.FullWindowedValueCoder) windowedInputCoder).getValueCoder();
      if (!(valueCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            String.format(
                Locale.ENGLISH,
                "The element coder for stateful DoFn '%s' must be KvCoder but is: %s",
                inputPCollectionId,
                valueCoder.getClass().getSimpleName()));
      }
      if (stateful) {
        keyCoder = ((KvCoder) valueCoder).getKeyCoder();
        keySelector =
            new KvToByteBufferKeySelector(
                keyCoder, new SerializablePipelineOptions(context.getPipelineOptions()));
      } else {
        // For an SDF, we know that the input element should be
        // KV<KV<element, KV<restriction, watermarkState>>, size>. We are going to use the element
        // as the key.
        if (!(((KvCoder) valueCoder).getKeyCoder() instanceof KvCoder)) {
          throw new IllegalStateException(
              String.format(
                  Locale.ENGLISH,
                  "The element coder for splittable DoFn '%s' must be KVCoder(KvCoder, DoubleCoder) but is: %s",
                  inputPCollectionId,
                  valueCoder.getClass().getSimpleName()));
        }
        keyCoder = ((KvCoder) ((KvCoder) valueCoder).getKeyCoder()).getKeyCoder();
        keySelector =
            new SdfByteBufferKeySelector(
                keyCoder, new SerializablePipelineOptions(context.getPipelineOptions()));
      }
      inputDataStream = inputDataStream.keyBy(keySelector);
    }

    DoFnOperator.MultiOutputOutputManagerFactory<OutputT> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainOutputTag,
            tagsToOutputTags,
            tagsToCoders,
            tagsToIds,
            new SerializablePipelineOptions(context.getPipelineOptions()));

    DoFnOperator<InputT, OutputT> doFnOperator =
        new ExecutableStageDoFnOperator<>(
            transform.getUniqueName(),
            windowedInputCoder,
            Collections.emptyMap(),
            mainOutputTag,
            additionalOutputTags,
            outputManagerFactory,
            transformedSideInputs.unionTagToView,
            new ArrayList<>(transformedSideInputs.unionTagToView.values()),
            getSideInputIdToPCollectionViewMap(stagePayload, components),
            context.getPipelineOptions(),
            stagePayload,
            context.getJobInfo(),
            FlinkExecutableStageContextFactory.getInstance(),
            collectionIdToTupleTag,
            getWindowingStrategy(inputPCollectionId, components),
            keyCoder,
            keySelector);

    final String operatorName = generateNameFromStagePayload(stagePayload);

    if (transformedSideInputs.unionTagToView.isEmpty()) {
      outputStream = inputDataStream.transform(operatorName, outputTypeInformation, doFnOperator);
    } else {
      DataStream<RawUnionValue> sideInputStream =
          transformedSideInputs.unionedSideInputs.broadcast();
      if (stateful || hasSdfProcessFn) {
        // We have to manually construct the two-input transform because we're not
        // allowed to have only one input keyed, normally. Since Flink 1.5.0 it's
        // possible to use the Broadcast State Pattern which provides a more elegant
        // way to process keyed main input with broadcast state, but it's not feasible
        // here because it breaks the DoFnOperator abstraction.
        TwoInputTransformation<WindowedValue<KV<?, InputT>>, RawUnionValue, WindowedValue<OutputT>>
            rawFlinkTransform =
                new TwoInputTransformation(
                    inputDataStream.getTransformation(),
                    sideInputStream.getTransformation(),
                    transform.getUniqueName(),
                    doFnOperator,
                    outputTypeInformation,
                    inputDataStream.getParallelism());

        rawFlinkTransform.setStateKeyType(((KeyedStream) inputDataStream).getKeyType());
        rawFlinkTransform.setStateKeySelectors(
            ((KeyedStream) inputDataStream).getKeySelector(), null);

        outputStream =
            new SingleOutputStreamOperator(
                inputDataStream.getExecutionEnvironment(),
                rawFlinkTransform) {}; // we have to cheat around the ctor being protected
      } else {
        outputStream =
            inputDataStream
                .connect(sideInputStream)
                .transform(operatorName, outputTypeInformation, doFnOperator);
      }
    }
    // Assign a unique but consistent id to re-map operator state
    outputStream.uid(transform.getUniqueName());

    if (mainOutputTag != null) {
      context.addDataStream(outputs.get(mainOutputTag.getId()), outputStream);
    }

    for (TupleTag<?> tupleTag : additionalOutputTags) {
      context.addDataStream(
          outputs.get(tupleTag.getId()),
          outputStream.getSideOutput(tagsToOutputTags.get(tupleTag)));
    }
  }
}
