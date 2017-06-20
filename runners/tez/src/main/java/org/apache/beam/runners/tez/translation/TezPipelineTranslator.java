package org.apache.beam.runners.tez.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.tez.TezPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TezPipelineTranslator} translates {@link Pipeline} objects
 * into Tez logical plan {@link DAG}.
 */
public class TezPipelineTranslator implements Pipeline.PipelineVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(TezPipelineTranslator.class);

  /**
   * A map from {@link PTransform} subclass to the corresponding
   * {@link TransformTranslator} to use to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, TransformTranslator>
      transformTranslators = new HashMap<>();

  private static final Map<Class<? extends PTransform>, TransformTranslator>
      compositeTransformTranslators = new HashMap<>();

  private final TranslationContext translationContext;

  static {
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoTranslator<>());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator<>());
    registerTransformTranslator(Window.Assign.class, new WindowAssignTranslator<>());
    registerTransformTranslator(Read.Bounded.class, new ReadBoundedTranslator<>());
    registerTransformTranslator(Flatten.PCollections.class, new FlattenPCollectionTranslator<>());
    registerTransformTranslator(View.CreatePCollectionView.class, new ViewCreatePCollectionViewTranslator<>());
    registerCompositeTransformTranslator(WriteFiles.class, new WriteFilesTranslator());
  }

  public TezPipelineTranslator(TezPipelineOptions options, TezConfiguration config){
    translationContext = new TranslationContext(options, config);
  }

  public void translate(Pipeline pipeline, DAG dag) {
    pipeline.traverseTopologically(this);
    translationContext.populateDAG(dag);
  }

  /**
   * Main visitor method called on each {@link PTransform} to transform them to Tez objects.
   * @param node Pipeline node containing {@link PTransform} to be translated.
   */
  @Override
  public void visitPrimitiveTransform(Node node) {
    LOG.debug("visiting transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    TransformTranslator translator = transformTranslators.get(transform.getClass());
    if (translator == null) {
      throw new UnsupportedOperationException(
          "no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node);
    translator.translate(transform, translationContext);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    LOG.debug("entering composite transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    if (transform != null){
      TransformTranslator translator = compositeTransformTranslators.get(transform.getClass());
      if (translator != null) {
        translationContext.setCurrentTransform(node);
        translator.translate(transform, translationContext);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(Node node) {
    LOG.debug("leaving composite transform {}", node.getTransform());
  }

  @Override
  public void visitValue(PValue value, Node producer) {
    LOG.debug("visiting value {}", value);
  }

  /**
   * Records that instances of the specified PTransform class
   * should be translated by default by the corresponding
   * {@link TransformTranslator}.
   */
  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass, TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }

  private static <TransformT extends PTransform> void registerCompositeTransformTranslator(
      Class<TransformT> transformClass, TransformTranslator<? extends TransformT> transformTranslator) {
    if (compositeTransformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }
}
