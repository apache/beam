package org.apache.beam.runners.samza.translation;

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.samza.operators.MessageStream;

/**
 * Helper that keeps the mapping from BEAM PCollection id to Samza {@link MessageStream}. It also
 * provides other context data such as input and output of a {@link
 * org.apache.beam.model.pipeline.v1.RunnerApi.PTransform}.
 */
public class PortableTranslationContext {
  private final Map<String, MessageStream<?>> messsageStreams = new HashMap<>();

  public MessageStream<OpMessage<?>> getInputMessageStream(PipelineNode.PTransformNode transform) {
    String id = Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    return (MessageStream<OpMessage<?>>) messsageStreams.get(id);
  }

  public String getOutputId(PipelineNode.PTransformNode transform) {
    return Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());
  }

  public void registerMessageStream(String id, MessageStream<OpMessage<?>> stream) {
    if (messsageStreams.containsKey(id)) {
      throw new IllegalArgumentException("Stream already registered for id: " + id);
    }

    messsageStreams.put(id, stream);
  }
}
