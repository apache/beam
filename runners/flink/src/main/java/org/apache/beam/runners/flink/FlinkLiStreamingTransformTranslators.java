package org.apache.beam.runners.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FlinkLiStreamingTransformTranslators {
  // --------------------------------------------------------------------------------------------
  //  LinkedIn Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  /** A map from a Transform URN to the translator. */
  @SuppressWarnings("rawtypes")
  private static final Map<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator>
      TRANSLATORS = new HashMap<>();

  // here you can find all the available LinkedIn translators.
  static {
    for (FlinkCustomTransformRegistrar registrar : ServiceLoader.load(FlinkCustomTransformRegistrar.class)) {
      for (String urn : registrar.getTransformPayloadTranslators().keySet()) {
        TRANSLATORS.put(urn, registrar.getTransformPayloadTranslators().get(urn));
      }
    }
  }

  public static FlinkStreamingPipelineTranslator.StreamTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform) {
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSLATORS.get(urn);
  }
}
