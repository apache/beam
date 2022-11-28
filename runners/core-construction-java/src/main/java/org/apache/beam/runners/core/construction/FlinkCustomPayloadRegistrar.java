package org.apache.beam.runners.core.construction;

import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;


@SuppressWarnings({
    "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public interface FlinkCustomPayloadRegistrar {
  Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
      getTransformPayloadTranslators();
}
