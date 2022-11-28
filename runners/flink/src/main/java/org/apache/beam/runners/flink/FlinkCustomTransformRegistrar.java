package org.apache.beam.runners.flink;

import java.util.Map;


public interface FlinkCustomTransformRegistrar {
  Map<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator<?>> getTransformPayloadTranslators();
}
