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

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.instantiateCoder;

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
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.DoFnOp;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.runtime.PortableDoFnOp;
import org.apache.beam.runners.samza.runtime.SamzaDoFnInvokerRegistrar;
import org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils;
import org.apache.beam.runners.samza.util.StateUtils;
import org.apache.beam.runners.samza.util.WindowUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.RunnerPCollectionView;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.joda.time.Instant;

/**
 * Translates {@link org.apache.beam.sdk.transforms.ParDo.MultiOutput} or ExecutableStage in
 * portable api to Samza {@link DoFnOp}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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

    final Coder<?> keyCoder =
        StateUtils.isStateful(transform.getFn())
            ? ((KvCoder<?, ?>) input.getCoder()).getKeyCoder()
            : null;

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
    final ArrayList<Map.Entry<TupleTag<?>, PCollection<?>>> outputs =
        new ArrayList<>(node.getOutputs().entrySet());

    final Map<TupleTag<?>, Integer> tagToIndexMap = new HashMap<>();
    final Map<Integer, PCollection<?>> indexToPCollectionMap = new HashMap<>();

    for (int index = 0; index < outputs.size(); ++index) {
      final Map.Entry<TupleTag<?>, PCollection<?>> taggedOutput = outputs.get(index);
      tagToIndexMap.put(taggedOutput.getKey(), index);

      if (!(taggedOutput.getValue() instanceof PCollection)) {
        throw new IllegalArgumentException(
            "Expected side output to be PCollection, but was: " + taggedOutput.getValue());
      }
      final PCollection<?> sideOutputCollection = taggedOutput.getValue();
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

    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    final Map<String, String> stateIdToStoreMapping = new HashMap<>();
    for (String stateId : signature.stateDeclarations().keySet()) {
      final String transformFullName = node.getEnclosingNode().getFullName();
      final String storeId = ctx.getStoreIdGenerator().getId(stateId, transformFullName);
      stateIdToStoreMapping.put(stateId, storeId);
    }
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
            null,
            Collections.emptyMap(),
            doFnSchemaInformation,
            sideInputMapping,
            stateIdToStoreMapping);

    final MessageStream<OpMessage<InT>> mergedStreams;
    if (sideInputStreams.isEmpty()) {
      mergedStreams = inputStream;
    } else {
      MessageStream<OpMessage<InT>> mergedSideInputStreams =
          MessageStream.mergeAll(sideInputStreams).flatMap(new SideInputWatermarkFn());
      mergedStreams = inputStream.merge(Collections.singletonList(mergedSideInputStreams));
    }

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        mergedStreams.flatMapAsync(OpAdapter.adapt(op, ctx));

    for (int outputIndex : tagToIndexMap.values()) {
      @SuppressWarnings("unchecked")
      final MessageStream<OpMessage<OutT>> outputStream =
          taggedOutputStream
              .filter(
                  message ->
                      message.getType() != OpMessage.Type.ELEMENT
                          || message.getElement().getValue().getUnionTag() == outputIndex)
              .flatMapAsync(OpAdapter.adapt(new RawUnionValueToValue(), ctx));

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

    // Analyze side inputs
    final List<MessageStream<OpMessage<Iterable<?>>>> sideInputStreams = new ArrayList<>();
    final Map<SideInputId, PCollectionView<?>> sideInputMapping = new HashMap<>();
    final Map<String, PCollectionView<?>> idToViewMapping = new HashMap<>();
    final RunnerApi.Components components = stagePayload.getComponents();
    for (SideInputId sideInputId : stagePayload.getSideInputsList()) {
      final String sideInputCollectionId =
          components
              .getTransformsOrThrow(sideInputId.getTransformId())
              .getInputsOrThrow(sideInputId.getLocalName());
      final WindowingStrategy<?, BoundedWindow> windowingStrategy =
          WindowUtils.getWindowStrategy(sideInputCollectionId, components);
      final WindowedValue.WindowedValueCoder<?> coder =
          (WindowedValue.WindowedValueCoder) instantiateCoder(sideInputCollectionId, components);

      // Create a runner-side view
      final PCollectionView<?> view = createPCollectionView(sideInputId, coder, windowingStrategy);

      // Use GBK to aggregate the side inputs and then broadcast it out
      final MessageStream<OpMessage<Iterable<?>>> broadcastSideInput =
          groupAndBroadcastSideInput(
              sideInputId,
              sideInputCollectionId,
              components.getPcollectionsOrThrow(sideInputCollectionId),
              (WindowingStrategy) windowingStrategy,
              coder,
              ctx);

      sideInputStreams.add(broadcastSideInput);
      sideInputMapping.put(sideInputId, view);
      idToViewMapping.put(getSideInputUniqueId(sideInputId), view);
    }

    final Map<TupleTag<?>, Integer> tagToIndexMap = new HashMap<>();
    final Map<Integer, String> indexToIdMap = new HashMap<>();
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
              String collectionId = outputs.get(outputName);
              indexToIdMap.put(index.get(), collectionId);
              idToTupleTagMap.put(collectionId, tupleTag);
              index.incrementAndGet();
            });

    WindowedValue.WindowedValueCoder<InT> windowedInputCoder =
        WindowUtils.instantiateWindowedCoder(inputId, pipeline.getComponents());

    // TODO: support schema and side inputs for portable runner
    // Note: transform.getTransform() is an ExecutableStage, not ParDo, so we need to extract
    // these info from its components.
    final DoFnSchemaInformation doFnSchemaInformation = null;

    final RunnerApi.PCollection input = pipeline.getComponents().getPcollectionsOrThrow(inputId);
    final PCollection.IsBounded isBounded = SamzaPipelineTranslatorUtils.isBounded(input);

    // No key coder information required for handing the stateless stage or stage with user states
    // The key coder information is required for handing the stage with user timers
    final Coder<?> timerKeyCoder =
        stagePayload.getTimersCount() > 0
            ? ((KvCoder)
                    ((WindowedValue.FullWindowedValueCoder) windowedInputCoder).getValueCoder())
                .getKeyCoder()
            : null;

    final PortableDoFnOp<InT, OutT, RawUnionValue> op =
        new PortableDoFnOp<>(
            mainOutputTag,
            new NoOpDoFn<>(),
            timerKeyCoder,
            windowedInputCoder.getValueCoder(), // input coder not in use
            windowedInputCoder,
            Collections.emptyMap(), // output coders not in use
            new ArrayList<>(sideInputMapping.values()),
            new ArrayList<>(idToTupleTagMap.values()), // used by java runner only
            WindowUtils.getWindowStrategy(inputId, stagePayload.getComponents()),
            idToViewMapping,
            new DoFnOp.MultiOutputManagerFactory(tagToIndexMap),
            ctx.getTransformFullName(),
            ctx.getTransformId(),
            isBounded,
            true,
            stagePayload,
            ctx.getJobInfo(),
            idToTupleTagMap,
            doFnSchemaInformation,
            sideInputMapping,
            Collections.emptyMap());

    final MessageStream<OpMessage<InT>> mergedStreams;
    if (sideInputStreams.isEmpty()) {
      mergedStreams = inputStream;
    } else {
      MessageStream<OpMessage<InT>> mergedSideInputStreams =
          MessageStream.mergeAll(sideInputStreams).flatMap(new SideInputWatermarkFn());
      mergedStreams = inputStream.merge(Collections.singletonList(mergedSideInputStreams));
    }

    final MessageStream<OpMessage<RawUnionValue>> taggedOutputStream =
        mergedStreams.flatMapAsync(OpAdapter.adapt(op, ctx));

    for (int outputIndex : tagToIndexMap.values()) {
      @SuppressWarnings("unchecked")
      final MessageStream<OpMessage<OutT>> outputStream =
          taggedOutputStream
              .filter(
                  message ->
                      message.getType() != OpMessage.Type.ELEMENT
                          || message.getElement().getValue().getUnionTag() == outputIndex)
              .flatMapAsync(OpAdapter.adapt(new RawUnionValueToValue(), ctx));

      ctx.registerMessageStream(indexToIdMap.get(outputIndex), outputStream);
    }
  }

  @Override
  public Map<String, String> createConfig(
      ParDo.MultiOutput<InT, OutT> transform, TransformHierarchy.Node node, ConfigContext ctx) {
    final Map<String, String> config = new HashMap<>();
    final DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
    final SamzaPipelineOptions options = ctx.getPipelineOptions();

    // If a ParDo observes directly or indirectly with window, then this is a stateful ParDo
    // in this case, we will use RocksDB as system store.
    if (signature.processElement().observesWindow()) {
      config.putAll(ConfigBuilder.createRocksDBStoreConfig(options));
    }

    if (signature.usesState()) {
      // set up user state configs
      for (String stateId : signature.stateDeclarations().keySet()) {
        final String transformFullName = node.getEnclosingNode().getFullName();
        final String storeId = ctx.getStoreIdGenerator().getId(stateId, transformFullName);
        config.put(
            "stores." + storeId + ".factory", RocksDbKeyValueStorageEngineFactory.class.getName());
        config.put("stores." + storeId + ".key.serde", "byteArraySerde");
        config.put("stores." + storeId + ".msg.serde", "stateValueSerde");
        config.put("stores." + storeId + ".rocksdb.compression", "lz4");

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

  @Override
  public Map<String, String> createPortableConfig(
      PipelineNode.PTransformNode transform, SamzaPipelineOptions options) {

    final RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload =
          RunnerApi.ExecutableStagePayload.parseFrom(
              transform.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (!StateUtils.isStateful(stagePayload)) {
      return Collections.emptyMap();
    }

    final Map<String, String> config =
        new HashMap<>(ConfigBuilder.createRocksDBStoreConfig(options));
    for (RunnerApi.ExecutableStagePayload.UserStateId stateId : stagePayload.getUserStatesList()) {
      final String storeId = stateId.getLocalName();

      config.put(
          "stores." + storeId + ".factory", RocksDbKeyValueStorageEngineFactory.class.getName());
      config.put("stores." + storeId + ".key.serde", "byteArraySerde");
      config.put("stores." + storeId + ".msg.serde", "stateValueSerde");
      config.put("stores." + storeId + ".rocksdb.compression", "lz4");

      if (options.getStateDurable()) {
        config.put(
            "stores." + storeId + ".changelog", ConfigBuilder.getChangelogTopic(options, storeId));
      }
    }

    return config;
  }

  @SuppressWarnings("unchecked")
  private static final ViewFn<Iterable<WindowedValue<?>>, ?> VIEW_FN =
      (ViewFn)
          new PCollectionViews.MultimapViewFn<>(
              (PCollectionViews.TypeDescriptorSupplier<Iterable<WindowedValue<Void>>>)
                  () -> TypeDescriptors.iterables(new TypeDescriptor<WindowedValue<Void>>() {}),
              (PCollectionViews.TypeDescriptorSupplier<Void>) TypeDescriptors::voids);

  // This method follows the same way in Flink to create a runner-side Java
  // PCollectionView to represent a portable side input.
  private static PCollectionView<?> createPCollectionView(
      SideInputId sideInputId,
      WindowedValue.WindowedValueCoder<?> coder,
      WindowingStrategy<?, BoundedWindow> windowingStrategy) {

    return new RunnerPCollectionView<>(
        null,
        new TupleTag<>(sideInputId.getLocalName()),
        VIEW_FN,
        // TODO: support custom mapping fn
        windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
        windowingStrategy,
        coder.getValueCoder());
  }

  // Group the side input globally with a null key and then broadcast it
  // to all tasks.
  private static <SideInputT>
      MessageStream<OpMessage<Iterable<SideInputT>>> groupAndBroadcastSideInput(
          SideInputId sideInputId,
          String sideInputCollectionId,
          RunnerApi.PCollection sideInputPCollection,
          WindowingStrategy<SideInputT, BoundedWindow> windowingStrategy,
          WindowedValue.WindowedValueCoder<SideInputT> coder,
          PortableTranslationContext ctx) {
    final MessageStream<OpMessage<SideInputT>> sideInput =
        ctx.getMessageStreamById(sideInputCollectionId);
    final MessageStream<OpMessage<KV<Void, SideInputT>>> keyedSideInput =
        sideInput.map(
            opMessage -> {
              WindowedValue<SideInputT> wv = opMessage.getElement();
              return OpMessage.ofElement(wv.withValue(KV.of(null, wv.getValue())));
            });
    final WindowedValue.WindowedValueCoder<KV<Void, SideInputT>> kvCoder =
        coder.withValueCoder(KvCoder.of(VoidCoder.of(), coder.getValueCoder()));
    final MessageStream<OpMessage<KV<Void, Iterable<SideInputT>>>> groupedSideInput =
        GroupByKeyTranslator.doTranslatePortable(
            sideInputPCollection,
            keyedSideInput,
            windowingStrategy,
            kvCoder,
            new TupleTag<>("main output"),
            ctx);
    final MessageStream<OpMessage<Iterable<SideInputT>>> nonkeyGroupedSideInput =
        groupedSideInput.map(
            opMessage -> {
              WindowedValue<KV<Void, Iterable<SideInputT>>> wv = opMessage.getElement();
              return OpMessage.ofElement(wv.withValue(wv.getValue().getValue()));
            });
    final MessageStream<OpMessage<Iterable<SideInputT>>> broadcastSideInput =
        SamzaPublishViewTranslator.doTranslate(
            nonkeyGroupedSideInput,
            coder.withValueCoder(IterableCoder.of(coder.getValueCoder())),
            ctx.getTransformId(),
            getSideInputUniqueId(sideInputId),
            ctx.getPipelineOptions());

    return broadcastSideInput;
  }

  private static String getSideInputUniqueId(SideInputId sideInputId) {
    return sideInputId.getTransformId() + "-" + sideInputId.getLocalName();
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
