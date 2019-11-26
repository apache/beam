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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.HashIdGenerator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.TableDescriptor;

/**
 * Helper that keeps the mapping from BEAM PCollection id to Samza {@link MessageStream}. It also
 * provides other context data such as input and output of a {@link
 * org.apache.beam.model.pipeline.v1.RunnerApi.PTransform}.
 */
public class PortableTranslationContext {
  private final Map<String, MessageStream<?>> messsageStreams = new HashMap<>();
  private final StreamApplicationDescriptor appDescriptor;
  private final SamzaPipelineOptions options;
  private final Set<String> registeredInputStreams = new HashSet<>();
  private final Map<String, Table> registeredTables = new HashMap<>();
  private final HashIdGenerator idGenerator = new HashIdGenerator();

  private PipelineNode.PTransformNode currentTransform;

  public PortableTranslationContext(
      StreamApplicationDescriptor appDescriptor, SamzaPipelineOptions options) {
    this.appDescriptor = appDescriptor;
    this.options = options;
  }

  public SamzaPipelineOptions getSamzaPipelineOptions() {
    return this.options;
  }

  public <T> List<MessageStream<OpMessage<T>>> getAllInputMessageStreams(
      PipelineNode.PTransformNode transform) {
    final Collection<String> inputStreamIds = transform.getTransform().getInputsMap().values();
    return inputStreamIds.stream().map(this::<T>getMessageStreamById).collect(Collectors.toList());
  }

  public <T> MessageStream<OpMessage<T>> getOneInputMessageStream(
      PipelineNode.PTransformNode transform) {
    String id = Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    return getMessageStreamById(id);
  }

  @SuppressWarnings("unchecked")
  public <T> MessageStream<OpMessage<T>> getMessageStreamById(String id) {
    return (MessageStream<OpMessage<T>>) messsageStreams.get(id);
  }

  public String getInputId(PipelineNode.PTransformNode transform) {
    return Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
  }

  public String getOutputId(PipelineNode.PTransformNode transform) {
    return Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());
  }

  public <T> void registerMessageStream(String id, MessageStream<OpMessage<T>> stream) {
    if (messsageStreams.containsKey(id)) {
      throw new IllegalArgumentException("Stream already registered for id: " + id);
    }
    messsageStreams.put(id, stream);
  }

  /** Get output stream by output descriptor. */
  public <OutT> OutputStream<OutT> getOutputStream(OutputDescriptor<OutT, ?> outputDescriptor) {
    return appDescriptor.getOutputStream(outputDescriptor);
  }

  /** Register an input stream with certain config id. */
  public <T> void registerInputMessageStream(
      String id, InputDescriptor<KV<?, OpMessage<T>>, ?> inputDescriptor) {
    // we want to register it with the Samza graph only once per i/o stream
    final String streamId = inputDescriptor.getStreamId();
    if (registeredInputStreams.contains(streamId)) {
      return;
    }
    final MessageStream<OpMessage<T>> stream =
        appDescriptor.getInputStream(inputDescriptor).map(org.apache.samza.operators.KV::getValue);

    registerMessageStream(id, stream);
    registeredInputStreams.add(streamId);
  }

  public WindowedValue.WindowedValueCoder instantiateCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return (WindowedValue.WindowedValueCoder)
          WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public WindowingStrategy<?, BoundedWindow> getPortableWindowStrategy(
      PipelineNode.PTransformNode transform, QueryablePipeline pipeline) {
    String inputId = Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());

    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline
            .getComponents()
            .getWindowingStrategiesOrThrow(
                pipeline.getComponents().getPcollectionsOrThrow(inputId).getWindowingStrategyId());

    WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy =
          WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto),
          e);
    }

    @SuppressWarnings("unchecked")
    WindowingStrategy<?, BoundedWindow> ret =
        (WindowingStrategy<?, BoundedWindow>) windowingStrategy;
    return ret;
  }

  @SuppressWarnings("unchecked")
  public <K, V> Table<KV<K, V>> getTable(TableDescriptor<K, V, ?> tableDesc) {
    return registeredTables.computeIfAbsent(
        tableDesc.getTableId(), id -> appDescriptor.getTable(tableDesc));
  }

  public void setCurrentTransform(PipelineNode.PTransformNode currentTransform) {
    this.currentTransform = currentTransform;
  }

  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  public String getTransformFullName() {
    return currentTransform.getTransform().getUniqueName();
  }

  public String getTransformId() {
    return idGenerator.getId(currentTransform.getTransform().getUniqueName());
  }
}
