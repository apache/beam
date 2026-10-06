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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.IOException;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.WireCoderSetting;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.construction.ReadTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

/**
 * Translates the deprecated primitive {@code Read} URN ({@code beam:transform:read:v1}).
 *
 * <p>The runner converts every {@code Read} into this primitive before translation, rather than
 * letting it expand into the default splittable-DoFn wrapper, which it cannot execute; see {@code
 * KafkaStreamsRunner.prepareForTranslation}.
 *
 * <p>The topology is the same three-node shape as {@link ImpulseTranslator}: a {@code byte[]}
 * source on a per-transform bootstrap topic, since Kafka Streams will not start a topology with no
 * source topic and the records on it are ignored; the {@link ReadProcessor}; and a persistent state
 * store recording whether the read already fired, so a restart does not duplicate elements.
 *
 * <p>Elements are emitted in the runner-side wire form the downstream harness expects, so the
 * processor is handed both wire coders for the output PCollection — see {@link ReadProcessor}.
 * Bounded and unbounded sources are both supported, by {@link ReadProcessor} and {@link
 * UnboundedReadProcessor} respectively.
 */
class ReadTranslator implements PTransformTranslator {

  static final String SOURCE_SUFFIX = "-source";
  static final String STATE_STORE_SUFFIX = "-state";

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    // Read produces exactly one output PCollection; downstream consumers are separate PTransforms
    // whose inputs reference this PCollection id and are wired by their own translators.
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    try {
      RunnerApi.ReadPayload payload =
          RunnerApi.ReadPayload.parseFrom(transform.getSpec().getPayload());
      // The same URN carries both kinds of source; the payload says which, and they need different
      // processors. A bounded source is drained once and ends time; an unbounded one is polled
      // repeatedly and moves the watermark as its reader reports progress.
      if (payload.getIsBounded() == RunnerApi.IsBounded.Enum.UNBOUNDED) {
        addUnboundedReadNodes(
            transformId,
            ReadTranslation.unboundedSourceFromProto(payload),
            pipeline.getComponents(),
            outputPCollectionId,
            context);
      } else {
        addReadNodes(
            transformId,
            ReadTranslation.boundedSourceFromProto(payload),
            pipeline.getComponents(),
            outputPCollectionId,
            context);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read the source from transform " + transform.getUniqueName(), e);
    }
  }

  /**
   * Adds the source, {@link UnboundedReadProcessor}, and the store holding its checkpoint mark.
   *
   * <p>The store keeps encoded bytes rather than the mark itself, since the mark's coder comes from
   * the source and is only known here.
   */
  private <T, CheckpointT extends UnboundedSource.CheckpointMark> void addUnboundedReadNodes(
      String transformId,
      UnboundedSource<T, CheckpointT> source,
      RunnerApi.Components components,
      String outputPCollectionId,
      KafkaStreamsTranslationContext context) {
    PCollectionNode outputNode =
        PipelineNode.pCollection(
            outputPCollectionId, components.getPcollectionsOrThrow(outputPCollectionId));
    Coder<WindowedValue<T>> sdkWireCoder = sdkWireCoder(outputNode, components);
    Coder<WindowedValue<?>> runnerWireCoder = runnerWireCoder(outputNode, components);

    Topology topology = context.getTopology();
    String sourceNodeName = transformId + SOURCE_SUFFIX;
    String stateStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, STATE_STORE_SUFFIX);
    String bootstrapTopic = context.getReadBootstrapTopic(transformId);
    SerializablePipelineOptions options =
        new SerializablePipelineOptions(context.getPipelineOptions());
    // Split here rather than in the processor: splitting belongs to translation, where it happens
    // once for the pipeline instead of once per task instance, and the contract says nothing about
    // splitting a source that has already been split.
    UnboundedSource<T, CheckpointT> readableSource = singleSplitOf(source, context);
    Coder<CheckpointT> checkpointCoder = readableSource.getCheckpointMarkCoder();
    int maxElementsPerPoll = context.getPipelineOptions().getReadMaxElementsPerPoll();
    int checkpointEveryNPolls = context.getPipelineOptions().getReadCheckpointNumBundles();
    int maxPollTimeMs = context.getPipelineOptions().getReadMaxPollTimeMs();

    topology.addSource(
        sourceNodeName,
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer(),
        bootstrapTopic);
    topology.addProcessor(
        transformId,
        () ->
            new UnboundedReadProcessor<>(
                readableSource,
                options,
                sdkWireCoder,
                runnerWireCoder,
                checkpointCoder,
                stateStoreName,
                transformId,
                maxElementsPerPoll,
                checkpointEveryNPolls,
                maxPollTimeMs,
                context.getTerminationTracker()),
        sourceNodeName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName), Serdes.String(), Serdes.ByteArray()),
        transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }

  /**
   * Adds the source, {@link ReadProcessor}, and state store for the read. The type variable {@code
   * T} captures the {@link BoundedSource}'s element type so the processor and its wire coders are
   * built consistently.
   */
  private <T> void addReadNodes(
      String transformId,
      BoundedSource<T> source,
      RunnerApi.Components components,
      String outputPCollectionId,
      KafkaStreamsTranslationContext context) {
    PCollectionNode outputNode =
        PipelineNode.pCollection(
            outputPCollectionId, components.getPcollectionsOrThrow(outputPCollectionId));
    Coder<WindowedValue<T>> sdkWireCoder = sdkWireCoder(outputNode, components);
    Coder<WindowedValue<?>> runnerWireCoder = runnerWireCoder(outputNode, components);

    Topology topology = context.getTopology();
    String sourceNodeName = transformId + SOURCE_SUFFIX;
    String stateStoreName =
        KafkaStreamsTranslationContext.getStoreName(transformId, STATE_STORE_SUFFIX);
    String bootstrapTopic = context.getReadBootstrapTopic(transformId);
    SerializablePipelineOptions options =
        new SerializablePipelineOptions(context.getPipelineOptions());

    topology.addSource(
        sourceNodeName,
        Serdes.ByteArray().deserializer(),
        Serdes.ByteArray().deserializer(),
        bootstrapTopic);
    topology.addProcessor(
        transformId,
        () ->
            new ReadProcessor<>(
                source,
                options,
                sdkWireCoder,
                runnerWireCoder,
                stateStoreName,
                transformId,
                context.getTerminationTracker()),
        sourceNodeName);
    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(stateStoreName);
    topology.addStateStore(
        Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Boolean()), transformId);

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }

  /**
   * Splits an unbounded source into the single part this runner reads.
   *
   * <p>A source is not obliged to be readable in its unsplit form — {@code split} is where several
   * of them do their setup — so it is asked to split even though only one part is wanted. The count
   * passed to {@code split} is only a hint, so what comes back has to be checked: taking the first
   * of several splits would quietly drop whatever the others would have produced, which is data
   * loss rather than a missing feature, so it fails instead.
   */
  private static <T, CheckpointT extends UnboundedSource.CheckpointMark>
      UnboundedSource<T, CheckpointT> singleSplitOf(
          UnboundedSource<T, CheckpointT> source, KafkaStreamsTranslationContext context) {
    List<? extends UnboundedSource<T, CheckpointT>> splits;
    try {
      splits = source.split(1, context.getPipelineOptions());
    } catch (Exception e) {
      throw new RuntimeException("Failed to split unbounded source " + source, e);
    }
    if (splits.size() != 1) {
      throw new UnsupportedOperationException(
          "Unbounded source "
              + source
              + " split into "
              + splits.size()
              + " parts, but the Kafka Streams runner reads a source with a single reader and"
              + " would therefore drop the data of every part but the first. Reading several"
              + " splits in parallel is not supported yet; see"
              + " https://github.com/apache/beam/issues/18479.");
    }
    return splits.get(0);
  }

  /** The coder the SDK harness would use on the wire, keeping unknown element coders intact. */
  private static <T> Coder<WindowedValue<T>> sdkWireCoder(
      PCollectionNode outputNode, RunnerApi.Components components) {
    try {
      RunnerApi.Components.Builder builder = components.toBuilder();
      String coderId =
          WireCoders.addSdkWireCoder(outputNode, builder, WireCoderSetting.getDefaultInstance());
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<T>> coder =
          (Coder<WindowedValue<T>>)
              RehydratedComponents.forComponents(builder.build()).getCoder(coderId);
      return coder;
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to build the SDK wire coder for PCollection " + outputNode.getId(), e);
    }
  }

  /** The coder the runner uses on the wire, replacing unknown element coders with byte arrays. */
  private static Coder<WindowedValue<?>> runnerWireCoder(
      PCollectionNode outputNode, RunnerApi.Components components) {
    try {
      @SuppressWarnings("unchecked")
      Coder<WindowedValue<?>> coder =
          (Coder<WindowedValue<?>>)
              (Coder<?>) WireCoders.instantiateRunnerWireCoder(outputNode, components);
      return coder;
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to build the runner wire coder for PCollection " + outputNode.getId(), e);
    }
  }
}
