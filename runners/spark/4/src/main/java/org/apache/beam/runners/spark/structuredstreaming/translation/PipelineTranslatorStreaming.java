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
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorCommon;
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
 * Pipeline translator for streaming pipelines on Spark 4. It extends the common registry to reuse
 * the stateless single output ParDo, Window.Assign, Flatten and Reshuffle translators, which are
 * safe on a streaming Dataset. Every other primitive fails at translation, the batch translators
 * for them persist or collect the Dataset, which Spark rejects for streaming plans.
 */
@Internal
public class PipelineTranslatorStreaming extends PipelineTranslatorCommon {

  private static final String NOT_SUPPORTED =
      " is not supported by the Spark 4 streaming runner yet, see"
          + " https://github.com/apache/beam/issues/36841";

  /** Returns a {@link TransformTranslator} for the given {@link PTransform} if known. */
  @Override
  @Nullable
  protected <InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform) {

    if (transform instanceof SplittableParDo.PrimitiveUnboundedRead) {
      @SuppressWarnings("unchecked")
      TransformTranslator<InT, OutT, TransformT> read =
          (TransformTranslator<InT, OutT, TransformT>)
              (TransformTranslator<?, ?, ?>) new ReadUnboundedTranslator<>();
      return read;
    }

    if (transform instanceof SplittableParDo.PrimitiveBoundedRead) {
      throw new UnsupportedOperationException(
          "Bounded Read (Read.from(BoundedSource), Create with two or more elements)"
              + NOT_SUPPORTED);
    }

    if (transform instanceof Impulse) {
      throw new UnsupportedOperationException(
          "Impulse (Create with fewer than two elements, PAssert)" + NOT_SUPPORTED);
    }

    if (transform instanceof GroupByKey) {
      throw new UnsupportedOperationException("GroupByKey" + NOT_SUPPORTED);
    }

    if (transform instanceof Combine.PerKey) {
      throw new UnsupportedOperationException("Combine.perKey" + NOT_SUPPORTED);
    }

    if (transform instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput<?, ?> parDo = (ParDo.MultiOutput<?, ?>) transform;
      DoFnSignature signature = DoFnSignatures.signatureForDoFn(parDo.getFn());
      if (signature.usesState() || signature.usesTimers()) {
        throw new UnsupportedOperationException(
            "Stateful ParDo (" + signature.fnClass().getName() + ")" + NOT_SUPPORTED);
      }
      if (!parDo.getSideInputs().isEmpty()) {
        throw new UnsupportedOperationException(
            "ParDo with side inputs (" + signature.fnClass().getName() + ")" + NOT_SUPPORTED);
      }
      if (!parDo.getAdditionalOutputTags().getAll().isEmpty()) {
        throw new UnsupportedOperationException(
            "ParDo with additional outputs ("
                + signature.fnClass().getName()
                + ")"
                + NOT_SUPPORTED);
      }
    }

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
