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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.SamzaMetricOpFactory;
import org.apache.beam.runners.samza.metrics.SamzaTransformMetricRegistry;
import org.apache.beam.runners.samza.runtime.OpAdapter;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.HashIdGenerator;
import org.apache.beam.runners.samza.util.StoreIdGenerator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper that keeps the mapping from BEAM {@link PValue}/{@link PCollectionView} to Samza {@link
 * MessageStream}. It also provides other context data such as input and output of a {@link
 * PTransform}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "keyfor",
  "nullness"
}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class TranslationContext {
  private static final Logger LOG = LoggerFactory.getLogger(TranslationContext.class);
  private final StreamApplicationDescriptor appDescriptor;
  private final Map<PValue, MessageStream<?>> messsageStreams = new HashMap<>();
  private final Map<PCollectionView<?>, MessageStream<?>> viewStreams = new HashMap<>();
  private final Map<PValue, String> idMap;
  private final Map<String, MessageStream> registeredInputStreams = new HashMap<>();
  private final Map<String, Table> registeredTables = new HashMap<>();
  private final SamzaPipelineOptions options;
  private final HashIdGenerator idGenerator = new HashIdGenerator();
  private final StoreIdGenerator storeIdGenerator;
  private final SamzaTransformMetricRegistry samzaTransformMetricRegistry;
  private AppliedPTransform<?, ?, ?> currentTransform;

  public TranslationContext(
      StreamApplicationDescriptor appDescriptor,
      Map<PValue, String> idMap,
      Set<String> nonUniqueStateIds,
      SamzaPipelineOptions options) {
    this.appDescriptor = appDescriptor;
    this.idMap = idMap;
    this.options = options;
    this.storeIdGenerator = new StoreIdGenerator(nonUniqueStateIds);
    this.samzaTransformMetricRegistry = new SamzaTransformMetricRegistry();
  }

  public <OutT> void registerInputMessageStream(
      PValue pvalue, InputDescriptor<KV<?, OpMessage<OutT>>, ?> inputDescriptor) {
    registerInputMessageStreams(pvalue, Collections.singletonList(inputDescriptor));
  }

  /**
   * Function to register a merged messageStream of all input messageStreams to a PCollection.
   *
   * @param pvalue output of a transform
   * @param inputDescriptors a list of Samza InputDescriptors
   */
  public <OutT> void registerInputMessageStreams(
      PValue pvalue, List<? extends InputDescriptor<KV<?, OpMessage<OutT>>, ?>> inputDescriptors) {
    registerInputMessageStreams(pvalue, inputDescriptors, this::registerMessageStream);
  }

  protected <KeyT, OutT> void registerInputMessageStreams(
      KeyT key,
      List<? extends InputDescriptor<KV<?, OpMessage<OutT>>, ?>> inputDescriptors,
      BiConsumer<KeyT, MessageStream<OpMessage<OutT>>> registerFunction) {
    final Set<MessageStream<OpMessage<OutT>>> streamsToMerge = new HashSet<>();
    for (InputDescriptor<KV<?, OpMessage<OutT>>, ?> inputDescriptor : inputDescriptors) {
      final String streamId = inputDescriptor.getStreamId();
      // each streamId registered in map should already be add in messageStreamMap
      if (registeredInputStreams.containsKey(streamId)) {
        @SuppressWarnings("unchecked")
        MessageStream<OpMessage<OutT>> messageStream = registeredInputStreams.get(streamId);
        LOG.info(
            String.format(
                "Stream id %s has already been mapped to %s stream. Mapping %s to the same message stream.",
                streamId, messageStream, key));
        streamsToMerge.add(messageStream);
      } else {
        final MessageStream<OpMessage<OutT>> typedStream =
            getValueStream(appDescriptor.getInputStream(inputDescriptor));
        registeredInputStreams.put(streamId, typedStream);
        streamsToMerge.add(typedStream);
      }
    }

    registerFunction.accept(key, MessageStream.mergeAll(streamsToMerge));
  }

  public <OutT> void registerMessageStream(PValue pvalue, MessageStream<OpMessage<OutT>> stream) {
    if (messsageStreams.containsKey(pvalue)) {
      throw new IllegalArgumentException("Stream already registered for pvalue: " + pvalue);
    }
    messsageStreams.put(pvalue, stream);
  }

  // Add a dummy stream for use in special cases (TestStream, empty flatten)
  public MessageStream<OpMessage<String>> getDummyStream() {
    InputDescriptor<OpMessage<String>, ?> dummyInput =
        createDummyStreamDescriptor(UUID.randomUUID().toString());
    return appDescriptor.getInputStream(dummyInput);
  }

  public <OutT> MessageStream<OpMessage<OutT>> getMessageStream(PValue pvalue) {
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<OutT>> stream =
        (MessageStream<OpMessage<OutT>>) messsageStreams.get(pvalue);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for pvalue: " + pvalue);
    }
    return stream;
  }

  public <InT extends PValue, OutT extends PValue> void attachTransformMetricOp(
      PTransform<InT, OutT> transform,
      TransformHierarchy.Node node,
      SamzaMetricOpFactory.OpType opType) {
    final Boolean enableTransformMetrics = getPipelineOptions().getEnableTransformMetrics();
    final String transformURN = PTransformTranslation.urnForTransformOrNull(transform);

    // skip attach transform if user override is false or transform is not registered
    if (!enableTransformMetrics || transformURN == null) {
      return;
    }

    // skip attach transform if transform is reading from external sources
    if (isIOTransform(node, opType)) {
      return;
    }

    for (PValue pValue : getPValueForTransform(opType, transform, node)) {
      // skip attach transform if pValue is not registered i.e. if not translated with a samza
      // translator
      if (!messsageStreams.containsKey(pValue)) {
        LOG.debug(
            "Skip attach transform metric op for pValue: {} for transform: {}",
            pValue,
            getTransformFullName());
        continue;
      }

      // add another step for default metric computation
      getMessageStream(pValue)
          .flatMapAsync(
              OpAdapter.adapt(
                  SamzaMetricOpFactory.createMetricOp(
                      transformURN,
                      pValue.getName(),
                      getTransformFullName(),
                      opType,
                      samzaTransformMetricRegistry),
                  this));
    }
  }

  // Get the input or output PValue for a transform
  private <InT extends PValue, OutT extends PValue> List<PValue> getPValueForTransform(
      SamzaMetricOpFactory.OpType opType,
      @NonNull PTransform<InT, OutT> transform,
      @NonNull TransformHierarchy.Node node) {
    switch (opType) {
      case INPUT:
        {
          if (node.getInputs().size() > 1) {
            return node.getInputs().entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
          } else {
            return ImmutableList.of(getInput(transform));
          }
        }
      case OUTPUT:
        if (node.getOutputs().size() > 1) {
          return node.getOutputs().entrySet().stream()
              .map(Map.Entry::getValue)
              .collect(Collectors.toList());
        }
        return ImmutableList.of(getOutput(transform));
      default:
        throw new IllegalArgumentException("Unknown opType: " + opType);
    }
  }

  // Transforms that read or write to/from external sources are not supported
  private static boolean isIOTransform(
      @NonNull TransformHierarchy.Node node, SamzaMetricOpFactory.OpType opType) {
    switch (opType) {
      case INPUT:
        return node.getInputs().size() == 0;
      case OUTPUT:
        return node.getOutputs().size() == 0;
      default:
        throw new IllegalArgumentException("Unknown opType: " + opType);
    }
  }

  public <ElemT, ViewT> void registerViewStream(
      PCollectionView<ViewT> view, MessageStream<OpMessage<Iterable<ElemT>>> stream) {
    if (viewStreams.containsKey(view)) {
      throw new IllegalArgumentException("Stream already registered for view: " + view);
    }

    viewStreams.put(view, stream);
  }

  public <InT> MessageStream<OpMessage<InT>> getViewStream(PCollectionView<?> view) {
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<InT>> stream =
        (MessageStream<OpMessage<InT>>) viewStreams.get(view);
    if (stream == null) {
      throw new IllegalArgumentException("No stream registered for view: " + view);
    }
    return stream;
  }

  public <ViewT> String getViewId(PCollectionView<ViewT> view) {
    return getIdForPValue(view);
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  @SuppressWarnings("unchecked")
  public <InT extends PValue> InT getInput(PTransform<InT, ?> transform) {
    return (InT)
        Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(this.currentTransform));
  }

  @SuppressWarnings("unchecked")
  public <OutT extends PValue> OutT getOutput(PTransform<?, OutT> transform) {
    return (OutT) Iterables.getOnlyElement(this.currentTransform.getOutputs().values());
  }

  @SuppressWarnings("unchecked")
  public <OutT> TupleTag<OutT> getOutputTag(PTransform<?, ? extends PCollection<OutT>> transform) {
    return (TupleTag<OutT>) Iterables.getOnlyElement(this.currentTransform.getOutputs().keySet());
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return this.options;
  }

  public <OutT> OutputStream<OutT> getOutputStream(OutputDescriptor<OutT, ?> outputDescriptor) {
    return appDescriptor.getOutputStream(outputDescriptor);
  }

  @SuppressWarnings("unchecked")
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    return registeredTables.computeIfAbsent(
        tableDesc.getTableId(), id -> appDescriptor.getTable(tableDesc));
  }

  private static <T> MessageStream<T> getValueStream(MessageStream<KV<?, T>> input) {
    return input.map(KV::getValue);
  }

  public String getIdForPValue(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No id mapping for value: " + pvalue);
    }
    return id;
  }

  public String getTransformFullName() {
    return currentTransform.getFullName();
  }

  public String getTransformId() {
    return idGenerator.getId(getTransformFullName());
  }

  public StoreIdGenerator getStoreIdGenerator() {
    return storeIdGenerator;
  }

  /** The dummy stream created will only be used in Beam tests. */
  private static InputDescriptor<OpMessage<String>, ?> createDummyStreamDescriptor(String id) {
    final GenericSystemDescriptor dummySystem =
        new GenericSystemDescriptor(id, InMemorySystemFactory.class.getName());
    final GenericInputDescriptor<OpMessage<String>> dummyInput =
        dummySystem.getInputDescriptor(id, new NoOpSerde<>());
    dummyInput.withOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST);
    final Config config = new MapConfig(dummyInput.toConfig(), dummySystem.toConfig());
    final SystemFactory factory = new InMemorySystemFactory();
    final StreamSpec dummyStreamSpec = new StreamSpec(id, id, id, 1);
    factory.getAdmin(id, config).createStream(dummyStreamSpec);

    final SystemProducer producer = factory.getProducer(id, config, null);
    final SystemStream sysStream = new SystemStream(id, id);
    final Consumer<Object> sendFn =
        (msg) -> {
          producer.send(id, new OutgoingMessageEnvelope(sysStream, 0, null, msg));
        };
    final WindowedValue<String> windowedValue =
        WindowedValue.timestampedValueInGlobalWindow("dummy", new Instant());

    sendFn.accept(OpMessage.ofElement(windowedValue));
    sendFn.accept(new WatermarkMessage(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    sendFn.accept(new EndOfStreamMessage(null));
    return dummyInput;
  }
}
