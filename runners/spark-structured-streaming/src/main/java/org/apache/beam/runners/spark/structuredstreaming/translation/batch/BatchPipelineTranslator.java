package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;

/** {@link Pipeline.PipelineVisitor} for executing a {@link Pipeline} as a Spark batch job. */

public class BatchPipelineTranslator extends PipelineTranslator {


  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private static final Map<String, BatchTransformTranslator>
      TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN,
        new CombinePerKeyTranslatorBatch());
    TRANSLATORS
        .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslatorBatch());
    TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslatorBatch());

    TRANSLATORS
        .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionTranslatorBatch());

    TRANSLATORS
        .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslatorBatch());

    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslatorBatch());

    TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslatorBatch());
  }

  /** Returns a translator for the given node, if it is possible, otherwise null. */
  private static BatchTransformTranslator<?> getTranslator(TransformHierarchy.Node node) {
    @Nullable PTransform<?, ?> transform = node.getTransform();
    // Root of the graph is null
    if (transform == null) {
      return null;
    }
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return (urn == null) ? null : TRANSLATORS.get(urn);
  }


  @Override public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    return super.enterCompositeTransform(node);
    //TODO impl
  }


  @Override public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    super.visitPrimitiveTransform(node);
    //TODO impl
  }

  }
