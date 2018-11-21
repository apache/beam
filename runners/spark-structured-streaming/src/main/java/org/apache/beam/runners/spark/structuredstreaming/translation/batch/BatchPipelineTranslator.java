package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;

/** {@link Pipeline.PipelineVisitor} for executing a {@link Pipeline} as a Spark batch job. */

public class BatchPipelineTranslator extends PipelineTranslator {


  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<String, TransformTranslator> TRANSFORM_TRANSLATORS = new HashMap<>();

  static {
    TRANSFORM_TRANSLATORS.put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN,
        new BatchCombinePerKeyTranslator());
    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new BatchGroupByKeyTranslator());
    TRANSFORM_TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new BatchReshuffleTranslator());

    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new BatchFlattenPCollectionTranslator());

    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new BatchWindowAssignTranslator());

    TRANSFORM_TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new BatchParDoTranslator());

    TRANSFORM_TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new BatchReadSourceTranslator());
  }

  public BatchPipelineTranslator(SparkPipelineOptions options) {
    translationContext = new BatchTranslationContext(options);
  }

  /** Returns a translator for the given node, if it is possible, otherwise null. */
  @Override
  protected TransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    @Nullable PTransform<?, ?> transform = node.getTransform();
    // Root of the graph is null
    if (transform == null) {
      return null;
    }
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return (urn == null) ? null : TRANSFORM_TRANSLATORS.get(urn);
  }


}
