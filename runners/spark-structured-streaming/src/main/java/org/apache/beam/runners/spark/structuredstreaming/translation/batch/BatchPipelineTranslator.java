package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
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
  private static final Map<String, BatchTransformTranslator> TRANSFORM_TRANSLATORS = new HashMap<>();

  static {
    TRANSFORM_TRANSLATORS.put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN,
        new CombinePerKeyTranslatorBatch());
    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslatorBatch());
    TRANSFORM_TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslatorBatch());

    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionTranslatorBatch());

    TRANSFORM_TRANSLATORS
        .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoTranslatorBatch());

    TRANSFORM_TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslatorBatch());
  }
  private static final Logger LOG = LoggerFactory.getLogger(BatchPipelineTranslator.class);

  public BatchPipelineTranslator(SparkPipelineOptions options) {
    translationContext = new BatchTranslationContext(options);
  }

  /** Returns a translator for the given node, if it is possible, otherwise null. */
  private static BatchTransformTranslator<?> getTransformTranslator(TransformHierarchy.Node node) {
    @Nullable PTransform<?, ?> transform = node.getTransform();
    // Root of the graph is null
    if (transform == null) {
      return null;
    }
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return (urn == null) ? null : TRANSFORM_TRANSLATORS.get(urn);
  }


  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info("{} enterCompositeTransform- {}", genSpaces(depth), node.getFullName());
    depth++;

    BatchTransformTranslator<?> transformTranslator = getTransformTranslator(node);

    if (transformTranslator != null) {
      translateNode(node, transformTranslator);
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
    BatchTransformTranslator<?> transformTranslator = getTransformTranslator(node);
    if (transformTranslator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(node.getTransform());
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    translateNode(node, transformTranslator);
  }

  private <T extends PTransform<?, ?>> void translateNode(
      TransformHierarchy.Node node,
      BatchTransformTranslator<?> transformTranslator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) node.getTransform();

    @SuppressWarnings("unchecked")
    BatchTransformTranslator<T> typedTransformTranslator = (BatchTransformTranslator<T>) transformTranslator;

    // create the applied PTransform on the translationContext
    translationContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    typedTransformTranslator.translateNode(typedTransform, translationContext);
  }


}
