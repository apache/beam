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

import java.util.Collection;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.spark.sql.SparkSession;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PipelineTranslator} for executing a streaming {@link org.apache.beam.sdk.Pipeline} on
 * Spark 4.
 *
 * <p>This extends {@link PipelineTranslatorBatch} purely for reuse: {@link
 * PipelineTranslatorBatch#getTransformTranslator} and its private registry are the only way to
 * reach the (package-private) batch translators for {@code Impulse}, {@code Window.Assign}, {@code
 * Flatten}, {@code Reshuffle}, the bounded read, and stateless {@code ParDo}, all of which are
 * reused completely unchanged for streaming. This class only intercepts the handful of transforms
 * that need genuinely different, streaming-aware handling before falling back to {@code super}.
 */
@Internal
public class PipelineTranslatorStreaming extends PipelineTranslatorBatch {

  // --------------------------------------------------------------------------------------------
  //  Placeholders for WS-D2
  // --------------------------------------------------------------------------------------------
  //
  // WS-D2 replaces each of the three constants below with a real translator instance (constructed
  // in place, same as the batch registry does) and removes the corresponding TODO. No other change
  // to this class should be necessary to plug those in: the interception points that select them in
  // getTransformTranslator already exist and are marked below.

  // TODO(WS-D2): replace with `new ReadUnboundedTranslator<>()`.
  private static final TransformTranslator<?, ?, ?> READ_UNBOUNDED_PLACEHOLDER =
      new UnsupportedStreamingTranslator(
          "Unbounded read streaming translation not implemented yet (WS-D2)");

  // TODO(WS-D2): replace with `new GroupByKeyStreamingTranslator<>()`.
  private static final TransformTranslator<?, ?, ?> GROUP_BY_KEY_PLACEHOLDER =
      new UnsupportedStreamingTranslator(
          "GroupByKey streaming translation not implemented yet (WS-D2)");

  // TODO(WS-D2): replace with `new StatefulParDoStreamingTranslator<>()`.
  private static final TransformTranslator<?, ?, ?> STATEFUL_PAR_DO_PLACEHOLDER =
      new UnsupportedStreamingTranslator(
          "Stateful ParDo streaming translation not implemented yet (WS-D2)");

  /** Returns a {@link TransformTranslator} for the given {@link PTransform} if known. */
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  protected <InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform) {

    // TODO(WS-D2): swap for `if (transform instanceof SplittableParDo.PrimitiveUnboundedRead)`
    // returning `new ReadUnboundedTranslator<>()` (or a cached instance thereof).
    if (transform instanceof SplittableParDo.PrimitiveUnboundedRead) {
      return (TransformTranslator) READ_UNBOUNDED_PLACEHOLDER;
    }

    // TODO(WS-D2): swap for `if (transform instanceof GroupByKey)` returning
    // `new GroupByKeyStreamingTranslator<>()`.
    if (transform instanceof GroupByKey) {
      return (TransformTranslator) GROUP_BY_KEY_PLACEHOLDER;
    }

    // Deliberately never registered: leaving Combine.PerKey unhandled here makes Beam auto-expand
    // it into GroupByKey + ParDo, so the streaming translations above (and WS-D2's stateful ParDo
    // translator) take over the expanded primitives instead of the batch
    // CombinePerKeyTranslatorBatch, which has no streaming support.
    if (transform instanceof Combine.PerKey) {
      return null;
    }

    if (transform instanceof ParDo.MultiOutput) {
      DoFnSignature signature =
          DoFnSignatures.signatureForDoFn(((ParDo.MultiOutput<?, ?>) transform).getFn());
      // TODO(WS-D2): swap this branch's return for `new StatefulParDoStreamingTranslator<>()`.
      if (signature.usesState() || signature.usesTimers()) {
        return (TransformTranslator) STATEFUL_PAR_DO_PLACEHOLDER;
      }
      // Stateless ParDo falls through to super, reusing ParDoTranslatorBatch unchanged.
    }

    // Impulse, Window.Assign, Flatten, Reshuffle, the bounded read, and stateless ParDo: reused
    // unchanged from the batch registry.
    return super.getTransformTranslator(transform);
  }

  @Override
  protected EvaluationContext createEvaluationContext(
      Collection<? extends EvaluationContext.NamedDataset<?>> leaves,
      SparkSession session,
      SparkCommonPipelineOptions options) {
    return new StreamingEvaluationContext(leaves, session, options);
  }

  /**
   * A {@link TransformTranslator} standing in for a streaming translation that WS-D2 has not
   * implemented yet. Always throws {@link UnsupportedOperationException} as soon as the pipeline
   * traversal tries to translate the transform it is registered for.
   */
  private static final class UnsupportedStreamingTranslator
      extends TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> {
    private final String message;

    UnsupportedStreamingTranslator(String message) {
      super(0);
      this.message = message;
    }

    @Override
    protected void translate(PTransform<PInput, POutput> transform, Context cxt) {
      throw new UnsupportedOperationException(message);
    }
  }
}
