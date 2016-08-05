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

package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * Pipeline {@link SparkRunner.Evaluator} for Spark.
 */
public final class SparkPipelineEvaluator extends SparkRunner.Evaluator {

  private final EvaluationContext ctxt;

  public SparkPipelineEvaluator(EvaluationContext ctxt, SparkPipelineTranslator translator) {
    super(translator);
    this.ctxt = ctxt;
  }

  @Override
  protected <TransformT extends PTransform<? super PInput, POutput>>
  void doVisitTransform(TransformTreeNode
      node) {
    @SuppressWarnings("unchecked")
    TransformT transform = (TransformT) node.getTransform();
    @SuppressWarnings("unchecked")
    Class<TransformT> transformClass = (Class<TransformT>) (Class<?>) transform.getClass();
    @SuppressWarnings("unchecked") TransformEvaluator<TransformT> evaluator =
        (TransformEvaluator<TransformT>) translator.translate(transformClass);
    LOG.info("Evaluating {}", transform);
    AppliedPTransform<PInput, POutput, TransformT> appliedTransform =
        AppliedPTransform.of(node.getFullName(), node.getInput(), node.getOutput(), transform);
    ctxt.setCurrentTransform(appliedTransform);
    evaluator.evaluate(transform, ctxt);
    ctxt.setCurrentTransform(null);
  }
}
