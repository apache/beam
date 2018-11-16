package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;

public class BatchPipelineTranslator extends PipelineTranslator {


  @Override public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    return super.enterCompositeTransform(node);
  }


  @Override public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    super.visitPrimitiveTransform(node);
  }


}
