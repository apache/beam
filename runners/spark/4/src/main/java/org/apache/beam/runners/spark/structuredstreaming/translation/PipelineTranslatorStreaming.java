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
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.GroupByKeyStreamingTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.ReadUnboundedTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.StatefulParDoStreamingTranslator;
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

  /** Returns a {@link TransformTranslator} for the given {@link PTransform} if known. */
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  protected <InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform) {

    if (transform instanceof SplittableParDo.PrimitiveUnboundedRead) {
      return (TransformTranslator) new ReadUnboundedTranslator<>();
    }

    if (transform instanceof GroupByKey) {
      return (TransformTranslator) new GroupByKeyStreamingTranslator<>();
    }

    // Deliberately never registered: leaving Combine.PerKey unhandled here makes Beam auto-expand
    // it into GroupByKey + ParDo, so the streaming translations above take over the expanded
    // primitives instead of the batch CombinePerKeyTranslatorBatch, which has no streaming
    // support.
    if (transform instanceof Combine.PerKey) {
      return null;
    }

    if (transform instanceof ParDo.MultiOutput) {
      DoFnSignature signature =
          DoFnSignatures.signatureForDoFn(((ParDo.MultiOutput<?, ?>) transform).getFn());
      if (signature.usesState() || signature.usesTimers()) {
        return (TransformTranslator) new StatefulParDoStreamingTranslator<>();
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
}
