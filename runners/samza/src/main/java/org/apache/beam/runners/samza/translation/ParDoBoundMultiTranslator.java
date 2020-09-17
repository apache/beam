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
package org.apache.beam.runners.samza.translation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.runtime.SamzaDoFnInvokerRegistrar;
import org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.joda.time.Instant;

/**
 * Translates {@link org.apache.beam.sdk.transforms.ParDo.MultiOutput} or ExecutableStage in
 * portable api to Samza {@link DoFnOp}.
 */
class ParDoBoundMultiTranslator<InT, OutT>
    implements TransformTranslator<ParDo.MultiOutput<InT, OutT>>,
        TransformConfigGenerator<ParDo.MultiOutput<InT, OutT>> {

  private final SamzaDoFnInvokerRegistrar doFnInvokerRegistrar;

  ParDoBoundMultiTranslator() {
    final Iterator<SamzaDoFnInvokerRegistrar> invokerReg =
        ServiceLoader.load(SamzaDoFnInvokerRegistrar.class).iterator();
    doFnInvokerRegistrar = invokerReg.hasNext() ? Iterators.getOnlyElement(invokerReg) : null;
  }

  @Override
  public void translate(
      ParDo.MultiOutput<InT, OutT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    doTranslate(transform, node, ctx);
  }

  // static for serializing anonymous functions
  private static <InT, OutT> void doTranslate(
      ParDo.MultiOutput<InT, OutT> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    final PCollection<? extends InT> input = ctx.getInput(transform);
    final Map<TupleTag<?>, Coder<?>> outputCoders =
        ctx.getCurrentTransform().getOutputs().entrySet().stream()
            .filter(e -> e.getValue() instanceof PCollection)
            .collect(
                Collectors.toMap(e -> e.getKey(), e -> ((PCollection<?>) e.getValue()).getCoder()));

    boolean isStateful = DoFnSignatures.isStateful(transform.getFn());
    final Coder<?> keyCoder = isStateful ? ((KvCoder<?, ?>) input.getCoder()).getKeyCoder() : null;

    if (DoFnSignatures.isSplittable(transform.getFn())) {
      throw new UnsupportedOperationException("Splittable DoFn is not currently supported");
    }
    if (DoFnSignatures.requiresTimeSortedInput(transform.getFn())) {
      throw new UnsupportedOperationException(
          "@RequiresTimeSortedInput annotation is not currently supported");
    }

    final MessageStream<OpMessage<InT>> inputStream = ctx.getMessageStream(input);
    final List<MessageStream<OpMessage<InT>>> sideInputStreams =
        transform.getSideInputs().values().stream()
            .map(ctx::<InT>getViewStream)
            .collect(Collectors.toList());
    final ArrayList<Map.Entry<TupleTag<?>, PValue>> outputs =
        new ArrayList<>(node.getOutputs().entrySet());

    final Map<TupleTag<?>, Integer> tagToIndexMap = new HashMap<>();
    final Map<Integer, PCollection<?>> indexToPCollectionMap = new HashMap<>();

    for (int index = 0; index < outputs.size(); ++index) {
      final Map.Entry<TupleTag<?>, PValue> taggedOutput = outputs.get(index);
      tagToIndexMap.put(taggedOutput.getKey(), index);

      if (!(taggedOutput.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException(
            "Expected side output to be PCollection, but was: " + taggedOutput.getValue());
      }
      final PCollection<?> sideOutputCollection = (PCollection<?>) taggedOutput.getValue();
      indexToPCollectionMap.put(index, sideOutputCollection);
    }

    final HashMap<String, PCollectionView<?>> idToPValueMap = new HashMap<>();
    for (PCollectionView<?> view : transform.getSideInputs().values()) {
      idToPValueMap.put(ctx.getViewId(view), view);
    }

    DoFnSchemaInformation doFnSchemaInformation;
    doFnSchemaInformation = ParDoTranslation.getSchemaInformation(ctx.getCurrentTransform());

    Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(ctx.getCurrentTransform());

    final DoFnOp<InT, OutT, RawUnionValue> op =
        new DoFnOp<>(
            transform.getMainOutputTag(),
            transform.getFn(),
            keyCoder,
            (Coder<InT>) input.getCoder(),
            null,
            outputCoders,
            transform.getSideInputs().values(),
            transform.getAdditionalOutputTags().getAll(),
            input.getWindowingStrategy(),
            idToPValueMap,
            new DoFnOp.MultiOutputManagerFactory(tagToIndexMap),
            ctx.getTransformFullName(),
            ctx.getTransformId(),
            input.isBounded(),
            false,
            null,
            Collections.emptyMap(),
            doFnSchemaInformation,
            sideInputMapping);

    final MessageStream<OpMessage<InT>> mergedStreams;
    if (sideInputStreams.isEmpty()) {
      mergedStreams = inputStream;
    } else {
      MessageStream<OpMessage<InT>> mergedSideInputStreams =
          MessageStream.mergeAll(sideInputStreams).flatMap(new SideInputWatermarkFn());
      mergedStreams = inputStream.merge(Collections.singletonList(mergedSideInputStreams));
    }

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        mergedStreams.flatMap(OpAdapter.adapt(op));

    for (int outputIndex : tagToIndexMap.values()) {
      @SuppressWarnings("unchecked")
      final MessageStream<OpMessage<OutT>> outputStream =
          taggedOutputStream
              .filter(
                  message ->
                      message.getType() != OpMessage.Type.ELEMENT
                          || message.getElement().getValue().getUnionTag() == outputIndex)
              .flatMap(OpAdapter.adapt(new RawUnionValueToValue()));

      ctx.registerMessageStream(indexToPCollectionMap.get(outputIndex), outputStream);
    }
  }

  /*
   * We reuse ParDo translator to translate ExecutableStage
   */
  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    doTranslatePortable(transform, pipeline, ctx);
  }

  // static for serializing anonymous functions
  private static <InT, OutT> void doTranslatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    Map<String, String> outputs = transform.getTransform().getOutputsMap();

    final RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String inputId = stagePayload.getInput();
    final MessageStream<OpMessage<InT>> inputStream = ctx.getMessageStreamById(inputId);
    // TODO: support side input
    final List<MessageStream<OpMessage<InT>>> sideInputStreams = Collections.emptyList();

    final Map<TupleTag<?>, Integer> tagToIndexMap = new HashMap<>();
    final Map<String, TupleTag<?>> idToTupleTagMap = new HashMap<>();

    // first output as the main output
    final TupleTag<OutT> mainOutputTag =
        outputs.isEmpty() ? null : new TupleTag(outputs.keySet().iterator().next());

    AtomicInteger index = new AtomicInteger(0);
    outputs
        .keySet()
        .iterator()
        .forEachRemaining(
            outputName -> {
              TupleTag<?> tupleTag = new TupleTag<>(outputName);
              tagToIndexMap.put(tupleTag, index.get());
              index.incrementAndGet();
              String collectionId = outputs.get(outputName);
              idToTupleTagMap.put(collectionId, tupleTag);
            });

    WindowedValue.WindowedValueCoder<InT> windowedInputCoder =
        ctx.instantiateCoder(inputId, pipeline.getComponents());

    final DoFnSchemaInformation doFnSchemaInformation;
    doFnSchemaInformation = ParDoTranslation.getSchemaInformation(transform.getTransform());

    Map<String, PCollectionView<?>> sideInputMapping =
        ParDoTranslation.getSideInputMapping(transform.getTransform());

    final RunnerApi.PCollection input = pipeline.getComponents().getPcollectionsOrThrow(inputId);
    final PCollection.IsBounded isBounded = SamzaPipelineTranslatorUtils.isBounded(input);

    final DoFnOp<InT, OutT, RawUnionValue> op =
        new DoFnOp<>(
            mainOutputTag,
            new NoOpDoFn<>(),
            null, // key coder not in use
            windowedInputCoder.getValueCoder(), // input coder not in use
            windowedInputCoder,
            Collections.emptyMap(), // output coders not in use
            Collections.emptyList(), // sideInputs not in use until side input support
            new ArrayList<>(idToTupleTagMap.values()), // used by java runner only
            SamzaPipelineTranslatorUtils.getPortableWindowStrategy(transform, pipeline),
            Collections.emptyMap(), // idToViewMap not in use until side input support
            new DoFnOp.MultiOutputManagerFactory(tagToIndexMap),
            ctx.getTransformFullName(),
            ctx.getTransformId(),
            isBounded,
            true,
            stagePayload,
            idToTupleTagMap,
            doFnSchemaInformation,
            sideInputMapping);

    final MessageStream<OpMessage<InT>> mergedStreams;
    if (sideInputStreams.isEmpty()) {
      mergedStreams = inputStream;
    } else {
      MessageStream<OpMessage<InT>> mergedSideInputStreams =
          MessageStream.mergeAll(sideInputStreams).flatMap(new SideInputWatermarkFn());
      mergedStreams = inputStream.merge(Collections.singletonList(mergedSideInputStreams));
    }

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        mergedStreams.flatMap(OpAdapter.adapt(op));

    for (int outputIndex : tagToIndexMap.values()) {
      final MessageStream<OpMessage<OutT>> outputStream =
          taggedOutputStream
              .filter(
                  message ->
                      message.getType() != OpMessage.Type.ELEMENT
                          || message.getElement().getValue().getUnionTag() == outputIndex)
              .flatMap(OpAdapter.adapt(new RawUnionValueToValue()));

      ctx.registerMessageStream(ctx.getOutputId(transform), outputStream);
    }
  }

  @Override
  public Map<String, String> createConfig(
      ParDo.MultiOutput<InT, OutT> transform, TransformHierarchy.Node node, ConfigContext ctx) {
    final Map<String, String> config = new HashMap<>();
    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    final SamzaPipelineOptions options = ctx.getPipelineOptions();

    if (signature.usesState()) {
      // set up user state configs
      for (DoFnSignature.StateDeclaration state : signature.stateDeclarations().values()) {
        final String storeId = state.id();
        config.put(
            "stores." + storeId + ".factory",
            "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        config.put("stores." + storeId + ".key.serde", "byteArraySerde");
        config.put("stores." + storeId + ".msg.serde", "byteSerde");

        if (options.getStateDurable()) {
          config.put(
              "stores." + storeId + ".changelog",
              ConfigBuilder.getChangelogTopic(options, storeId));
        }
      }
    }

    if (doFnInvokerRegistrar != null) {
      config.putAll(doFnInvokerRegistrar.configFor(transform.getFn()));
    }

    return config;
  }

  static class SideInputWatermarkFn<InT>
      implements FlatMapFunction<OpMessage<InT>, OpMessage<InT>>,
          WatermarkFunction<OpMessage<InT>> {

    @Override
    public Collection<OpMessage<InT>> apply(OpMessage<InT> message) {
      return Collections.singletonList(message);
    }

    @Override
    public Collection<OpMessage<InT>> processWatermark(long watermark) {
      return Collections.singletonList(OpMessage.ofSideInputWatermark(new Instant(watermark)));
    }

    @Override
    public Long getOutputWatermark() {
      // Always return max so the side input watermark will not be aggregated with main inputs.
      return Long.MAX_VALUE;
    }
  }

  static class RawUnionValueToValue<OutT> implements Op<RawUnionValue, OutT, Void> {
    @Override
    public void processElement(WindowedValue<RawUnionValue> inputElement, OpEmitter<OutT> emitter) {
      @SuppressWarnings("unchecked")
      final OutT value = (OutT) inputElement.getValue().getValue();
      emitter.emitElement(inputElement.withValue(value));
    }
  }

  private static class NoOpDoFn<InT, OutT> extends DoFn<InT, OutT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }
}
