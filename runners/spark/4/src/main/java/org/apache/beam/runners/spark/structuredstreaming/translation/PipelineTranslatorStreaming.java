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
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.ReadUnboundedTranslator;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
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
 * Pipeline translator for streaming pipelines on Spark 4. It extends the batch translator to reuse
 * the stateless single output ParDo, Window.Assign, Flatten and Reshuffle translators, which are
 * safe on a streaming Dataset. Every other primitive fails at translation, the batch translators
 * for them persist or collect the Dataset, which Spark rejects for streaming plans.
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

    if (transform instanceof SplittableParDo.PrimitiveBoundedRead) {
      throw unsupported(
          "Bounded Read (Read.from(BoundedSource), Create with two or more elements)");
    }

    if (transform instanceof Impulse) {
      throw unsupported("Impulse (Create with fewer than two elements, PAssert)");
    }

    if (transform instanceof GroupByKey) {
      throw unsupported("GroupByKey");
    }

    if (transform instanceof Combine.PerKey) {
      throw unsupported("Combine.perKey");
    }

    if (transform instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput<?, ?> parDo = (ParDo.MultiOutput<?, ?>) transform;
      DoFnSignature signature = DoFnSignatures.signatureForDoFn(parDo.getFn());
      if (signature.usesState() || signature.usesTimers()) {
        throw unsupported("Stateful ParDo (" + signature.fnClass().getName() + ")");
      }
      if (!parDo.getSideInputs().isEmpty()) {
        throw unsupported("ParDo with side inputs (" + signature.fnClass().getName() + ")");
      }
      if (!parDo.getAdditionalOutputTags().getAll().isEmpty()) {
        throw unsupported("ParDo with additional outputs (" + signature.fnClass().getName() + ")");
      }
    }

    return super.getTransformTranslator(transform);
  }

  private static UnsupportedOperationException unsupported(String what) {
    return new UnsupportedOperationException(
        what
            + " is not supported by the Spark 4 streaming runner yet, see"
            + " https://github.com/apache/beam/issues/36841");
  }

  @Override
  protected EvaluationContext createEvaluationContext(
      Collection<? extends EvaluationContext.NamedDataset<?>> leaves,
      SparkSession session,
      SparkCommonPipelineOptions options) {
    return new StreamingEvaluationContext(leaves, session, options);
  }
}
