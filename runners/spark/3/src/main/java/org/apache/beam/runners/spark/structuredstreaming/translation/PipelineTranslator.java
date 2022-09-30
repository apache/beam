/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.io.IOException;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Pipeline.PipelineVisitor} that translates the Beam operators to their Spark counterparts.
 * It also does the pipeline preparation: mode detection, transforms replacement, classpath
 * preparation.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class PipelineTranslator extends Pipeline.PipelineVisitor.Defaults {
  private int depth = 0;
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class);
  protected TranslationContext translationContext;

  // --------------------------------------------------------------------------------------------
  //  Pipeline preparation methods
  // --------------------------------------------------------------------------------------------
  public static void replaceTransforms(Pipeline pipeline, StreamingOptions options) {
    pipeline.replaceAll(SparkTransformOverrides.getDefaultOverrides(options.isStreaming()));
  }

  /**
   * Visit the pipeline to determine the translation mode (batch/streaming) and update options
   * accordingly.
   */
  public static void detectTranslationMode(Pipeline pipeline, StreamingOptions options) {
    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      options.setStreaming(true);
    }
  }

  /** The translation mode of the Beam Pipeline. */
  private enum TranslationMode {

    /** Uses the batch mode. */
    BATCH,

    /** Uses the streaming mode. */
    STREAMING
  }

  /** Traverses the Pipeline to determine the {@link TranslationMode} for this pipeline. */
  private static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(TranslationModeDetector.class);

    private TranslationMode translationMode;

    TranslationModeDetector(TranslationMode defaultMode) {
      this.translationMode = defaultMode;
    }

    TranslationModeDetector() {
      this(TranslationMode.BATCH);
    }

    TranslationMode getTranslationMode() {
      return translationMode;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (translationMode.equals(TranslationMode.BATCH)) {
        if (value instanceof PCollection
            && ((PCollection) value).isBounded() == PCollection.IsBounded.UNBOUNDED) {
          LOG.info(
              "Found unbounded PCollection {}. Switching to streaming execution.", value.getName());
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline utility methods
  // --------------------------------------------------------------------------------------------

  /**
   * Utility formatting method.
   *
   * @param n number of spaces to generate
   * @return String with "|" followed by n spaces
   */
  private static String genSpaces(int n) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < n; i++) {
      builder.append("|   ");
    }
    return builder.toString();
  }

  /** Get a {@link TransformTranslator} for the given {@link TransformHierarchy.Node}. */
  protected abstract @Nullable <
          InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(
          @Nullable TransformT transform);

  /** Apply the given TransformTranslator to the given node. */
  private <InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      void applyTransformTranslator(
          TransformHierarchy.Node node,
          TransformT transform,
          TransformTranslator<InT, OutT, TransformT> transformTranslator) {
    // create the applied PTransform on the translationContext
    AppliedPTransform<InT, OutT, PTransform<InT, OutT>> appliedTransform =
        (AppliedPTransform) node.toAppliedPTransform(getPipeline());
    try {
      transformTranslator.translate(transform, appliedTransform, translationContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline visitor entry point
  // --------------------------------------------------------------------------------------------

  /**
   * Translates the pipeline by passing this class as a visitor.
   *
   * @param pipeline The pipeline to be translated
   */
  public void translate(Pipeline pipeline) {
    LOG.debug("starting translation of the pipeline using {}", getClass().getName());
    pipeline.traverseTopologically(this);
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug("{} enterCompositeTransform- {}", genSpaces(depth), node.getFullName());
    depth++;

    PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
    TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> transformTranslator =
        getTransformTranslator(transform);

    if (transformTranslator != null) {
      applyTransformTranslator(node, transform, transformTranslator);
      LOG.debug("{} translated- {}", genSpaces(depth), node.getFullName());
      return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    } else {
      return CompositeBehavior.ENTER_TRANSFORM;
    }
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    depth--;
    LOG.debug("{} leaveCompositeTransform- {}", genSpaces(depth), node.getFullName());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.debug("{} visitPrimitiveTransform- {}", genSpaces(depth), node.getFullName());

    // get the transformation corresponding to the node we are
    // currently visiting and translate it into its Spark alternative.
    PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
    TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> transformTranslator =
        getTransformTranslator(transform);

    if (transformTranslator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(node.getTransform());
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    applyTransformTranslator(node, transform, transformTranslator);
  }

  public TranslationContext getTranslationContext() {
    return translationContext;
  }
}
