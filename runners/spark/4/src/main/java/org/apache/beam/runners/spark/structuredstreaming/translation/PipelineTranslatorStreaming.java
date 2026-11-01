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
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.GroupByKeyStreamingTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.ReadUnboundedTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.StatefulParDoStreamingTranslator;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.spark.sql.SparkSession;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pipeline translator for streaming pipelines on Spark 4. It extends the common registry to reuse
 * the stateless single output ParDo, Window.Assign, Flatten, Reshuffle and Combine.GroupedValues
 * translators, which are safe on a streaming Dataset. Unsupported primitives fail at translation.
 */
@Internal
public class PipelineTranslatorStreaming extends PipelineTranslatorCommon {

  @SuppressWarnings("rawtypes")
  private static final TransformTranslator READ_UNBOUNDED = new ReadUnboundedTranslator<>();

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static final TransformTranslator STATEFUL_PAR_DO =
      new StatefulParDoStreamingTranslator<Object, Object, Object>() {
        @Override
        protected void translate(
            ParDo.MultiOutput<KV<Object, Object>, Object> transform, Context cxt) {
          PCollection<?> input = (PCollection<?>) cxt.getInput();
          checkNotMergingWindows(input.getWindowingStrategy(), "Stateful ParDo");
          super.translate(transform, cxt);
        }
      };

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static final TransformTranslator GROUP_BY_KEY =
      new GroupByKeyStreamingTranslator<Object, Object>() {
        @Override
        protected void translate(GroupByKey<Object, Object> transform, Context cxt) {
          PCollection<?> input = (PCollection<?>) cxt.getInput();
          checkSupportedGroupByKey(input);
          super.translate(transform, cxt);
        }
      };

  private static final String NOT_SUPPORTED =
      " is not supported by the Spark 4 streaming runner yet, see"
          + " https://github.com/apache/beam/issues/36841";

  /** Returns a {@link TransformTranslator} for the given {@link PTransform} if known. */
  @Override
  @Nullable
  protected <InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform) {

    if (transform instanceof SplittableParDo.PrimitiveUnboundedRead) {
      return READ_UNBOUNDED;
    }

    if (transform instanceof GroupByKey) {
      return GROUP_BY_KEY;
    }

    if (transform instanceof Combine.Globally
        && ((Combine.Globally<?, ?>) transform).isInsertDefault()) {
      throw new UnsupportedOperationException(
          "Combine.globally() with a default value (use withoutDefaults())" + NOT_SUPPORTED);
    }

    if (transform instanceof Combine.PerKey || transform instanceof Combine.Globally) {
      return null;
    }

    if (transform instanceof ParDo.MultiOutput) {
      ParDo.MultiOutput<?, ?> parDo = (ParDo.MultiOutput<?, ?>) transform;
      DoFnSignature signature = DoFnSignatures.signatureForDoFn(parDo.getFn());
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
      if (signature.processElement().requiresTimeSortedInput()) {
        throw new UnsupportedOperationException(
            "@RequiresTimeSortedInput (" + signature.fnClass().getName() + ")" + NOT_SUPPORTED);
      }
      if (signature.onWindowExpiration() != null) {
        throw new UnsupportedOperationException(
            "@OnWindowExpiration (" + signature.fnClass().getName() + ")" + NOT_SUPPORTED);
      }
      checkNoProcessingTimeTimers(parDo.getFn(), signature);

      if (signature.usesState() || signature.usesTimers()) {
        return STATEFUL_PAR_DO;
      }
    }

    return super.getTransformTranslator(transform);
  }

  private static void checkNoProcessingTimeTimers(DoFn<?, ?> doFn, DoFnSignature signature) {
    for (DoFnSignature.TimerDeclaration timer : signature.timerDeclarations().values()) {
      TimerSpec spec = DoFnSignatures.getTimerSpecOrThrow(timer, doFn);
      if (spec.getTimeDomain() != TimeDomain.EVENT_TIME) {
        throw new UnsupportedOperationException(
            spec.getTimeDomain() + " timer @TimerId(\"" + timer.id() + "\")" + NOT_SUPPORTED);
      }
    }
    for (DoFnSignature.TimerFamilyDeclaration family :
        signature.timerFamilyDeclarations().values()) {
      TimerSpec spec = DoFnSignatures.getTimerFamilySpecOrThrow(family, doFn);
      if (spec.getTimeDomain() != TimeDomain.EVENT_TIME) {
        throw new UnsupportedOperationException(
            spec.getTimeDomain()
                + " timer family @TimerFamily(\""
                + family.id()
                + "\")"
                + NOT_SUPPORTED);
      }
    }
  }

  private static void checkSupportedGroupByKey(PCollection<?> input) {
    WindowingStrategy<?, ?> strategy = input.getWindowingStrategy();
    checkNotMergingWindows(strategy, "GroupByKey");
    if (!(strategy.getTrigger() instanceof DefaultTrigger
        || strategy.getTrigger() instanceof NeverTrigger)) {
      throw new UnsupportedOperationException(
          "Custom triggers (" + strategy.getTrigger() + ")" + NOT_SUPPORTED);
    }
    if (strategy.getMode() != AccumulationMode.DISCARDING_FIRED_PANES) {
      throw new UnsupportedOperationException("Accumulating fired panes" + NOT_SUPPORTED);
    }
  }

  private static void checkNotMergingWindows(
      WindowingStrategy<?, ?> windowingStrategy, String feature) {
    if (windowingStrategy.needsMerge()) {
      throw new UnsupportedOperationException(feature + " over merging windows" + NOT_SUPPORTED);
    }
  }

  @Override
  protected EvaluationContext createEvaluationContext(
      Collection<? extends EvaluationContext.NamedDataset<?>> leaves,
      SparkSession session,
      SparkCommonPipelineOptions options) {
    return new StreamingEvaluationContext(leaves, session, options);
  }
}
