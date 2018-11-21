package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link Pipeline.PipelineVisitor} for executing a {@link Pipeline} as a Spark batch job. */

public class BatchPipelineTranslator extends PipelineTranslator {


  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  private BatchTranslationContext translationContext;
  private int depth = 0;

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
  private static final Logger LOG = LoggerFactory.getLogger(BatchPipelineTranslator.class);



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


  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info("{} enterCompositeTransform- {}", genSpaces(depth), node.getFullName());
    depth++;

    BatchTransformTranslator<?> translator = getTranslator(node);

    if (translator != null) {
      translateNode(node, translator);
      LOG.info("{} translated- {}", genSpaces(depth), node.getFullName());
      return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    } else {
      return CompositeBehavior.ENTER_TRANSFORM;
    }
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    depth--;
    LOG.info("{} leaveCompositeTransform- {}", genSpaces(depth), node.getFullName());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.info("{} visitPrimitiveTransform- {}", genSpaces(depth), node.getFullName());

    // get the transformation corresponding to the node we are
    // currently visiting and translate it into its Spark alternative.
    BatchTransformTranslator<?> translator = getTranslator(node);
    if (translator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(node.getTransform());
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    translateNode(node, translator);
  }

  private <T extends PTransform<?, ?>> void translateNode(
      TransformHierarchy.Node node,
      BatchTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) node.getTransform();

    @SuppressWarnings("unchecked")
    BatchTransformTranslator<T> typedTranslator = (BatchTransformTranslator<T>) translator;

    // create the applied PTransform on the translationContext
    translationContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    typedTranslator.translateNode(typedTransform, translationContext);
  }


}
