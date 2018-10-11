package org.apache.beam.runners.samza.translation;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Portable specific samza pipeline translator. This is the entry point for translating a portable
 * pipeline
 */
public class SamzaPortablePipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPortablePipelineTranslator.class);

  private static final Map<String, TransformTranslator<?>> TRANSLATORS = loadTranslators();

  private static Map<String, TransformTranslator<?>> loadTranslators() {
    Map<String, TransformTranslator<?>> translators = new HashMap<>();
    for (SamzaTranslatorRegistrar registrar : ServiceLoader.load(SamzaTranslatorRegistrar.class)) {
      translators.putAll(registrar.getTransformTranslators());
    }
    LOG.info("{} translators loaded.", translators.size());
    return ImmutableMap.copyOf(translators);
  }

  private SamzaPortablePipelineTranslator() {}

  public static void translate(RunnerApi.Pipeline pipeline, PortableTranslationContext ctx) {
    QueryablePipeline queryablePipeline =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    int topologicalId = 0;
    for (PipelineNode.PTransformNode transform :
        queryablePipeline.getTopologicallyOrderedTransforms()) {
      ctx.setCurrentTopologicalId(topologicalId++);
      LOG.info("Translating transform urn: {}", transform.getTransform().getSpec().getUrn());
      TRANSLATORS
          .get(transform.getTransform().getSpec().getUrn())
          .translatePortable(transform, queryablePipeline, ctx);
    }
  }

  public static void createConfig(RunnerApi.Pipeline pipeline, ConfigBuilder configBuilder) {
    QueryablePipeline queryablePipeline =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform :
        queryablePipeline.getTopologicallyOrderedTransforms()) {
      TransformTranslator<?> translator =
          TRANSLATORS.get(transform.getTransform().getSpec().getUrn());
      if (translator instanceof TransformConfigGenerator) {
        TransformConfigGenerator configGenerator = (TransformConfigGenerator) translator;
        configBuilder.putAll(configGenerator.createPortableConfig(transform));
      }
    }
  }
}
