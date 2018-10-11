package org.apache.beam.runners.samza.translation;

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;

/**
 * Translate {@link org.apache.beam.sdk.transforms.Impulse} to a samza message stream produced by
 * {@link
 * org.apache.beam.runners.samza.translation.SamzaImpulseSystemFactory.SamzaImpulseSystemConsumer}.
 */
public class ImpulseTranslator implements TransformTranslator, TransformConfigGenerator {
  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    final String outputId = ctx.getOutputId(transform);
    ctx.registerInputMessageStream(outputId);
  }

  @Override
  public Map<String, String> createPortableConfig(PipelineNode.PTransformNode transform) {
    final String id = Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());

    final Map<String, String> config = new HashMap<>();
    final String systemPrefix = "systems." + id;
    final String streamPrefix = "streams." + id;

    config.put(systemPrefix + ".samza.factory", SamzaImpulseSystemFactory.class.getName());
    config.put(streamPrefix + ".samza.system", id);

    return config;
  }
}
