package org.apache.beam.runners.samza.translation;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;

/**
 * Helper that keeps the mapping from BEAM PCollection id to Samza {@link MessageStream}. It also
 * provides other context data such as input and output of a {@link
 * org.apache.beam.model.pipeline.v1.RunnerApi.PTransform}.
 */
public class PortableTranslationContext {
  private final Map<String, MessageStream<?>> messsageStreams = new HashMap<>();
  private final StreamGraph streamGraph;
  private final SamzaPipelineOptions options;
  private int topologicalId;

  public PortableTranslationContext(StreamGraph streamGraph, SamzaPipelineOptions options) {
    this.streamGraph = streamGraph;
    this.options = options;
  }

  public SamzaPipelineOptions getSamzaPipelineOptions() {
    return this.options;
  }

  public void setCurrentTopologicalId(int id) {
    this.topologicalId = id;
  }

  public int getCurrentTopologicalId() {
    return this.topologicalId;
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

  public <T> void registerInputMessageStream(String id) {
    final MessageStream<OpMessage<T>> stream =
        streamGraph
            .<org.apache.samza.operators.KV<?, OpMessage<T>>>getInputStream(id)
            .map(org.apache.samza.operators.KV::getValue);

    registerMessageStream(id, stream);
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
    } catch (InvalidProtocolBufferException e) {
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
}
