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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.HashIdGenerator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper that keeps the mapping from BEAM {@link PValue}/{@link PCollectionView} to Samza {@link
 * MessageStream}. It also provides other context data such as input and output of a {@link
 * PTransform}.
 */
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

  private AppliedPTransform<?, ?, ?> currentTransform;

  public TranslationContext(
      StreamApplicationDescriptor appDescriptor,
      Map<PValue, String> idMap,
      SamzaPipelineOptions options) {
    this.appDescriptor = appDescriptor;
    this.idMap = idMap;
    this.options = options;
  }

  public <OutT> void registerInputMessageStream(
      PValue pvalue,
      InputDescriptor<org.apache.samza.operators.KV<?, OpMessage<OutT>>, ?> inputDescriptor) {
    // we want to register it with the Samza graph only once per i/o stream
    final String streamId = inputDescriptor.getStreamId();
    if (registeredInputStreams.containsKey(streamId)) {
      MessageStream<OpMessage<OutT>> messageStream = registeredInputStreams.get(streamId);
      LOG.info(
          String.format(
              "Stream id %s has already been mapped to %s stream. Mapping %s to the same message stream.",
              streamId, messageStream, pvalue));
      registerMessageStream(pvalue, messageStream);

      return;
    }
    @SuppressWarnings("unchecked")
    final MessageStream<OpMessage<OutT>> typedStream =
        getValueStream(appDescriptor.getInputStream(inputDescriptor));

    registerMessageStream(pvalue, typedStream);
    registeredInputStreams.put(streamId, typedStream);
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

  private static <T> MessageStream<T> getValueStream(
      MessageStream<org.apache.samza.operators.KV<?, T>> input) {
    return input.map(org.apache.samza.operators.KV::getValue);
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
    return idGenerator.getId(currentTransform.getFullName());
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
