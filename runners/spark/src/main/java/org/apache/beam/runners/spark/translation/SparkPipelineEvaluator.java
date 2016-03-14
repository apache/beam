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

import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import org.apache.beam.runners.spark.SparkPipelineRunner;

/**
 * Pipeline {@link SparkPipelineRunner.Evaluator} for Spark.
 */
public final class SparkPipelineEvaluator extends SparkPipelineRunner.Evaluator {

  private final EvaluationContext ctxt;

  public SparkPipelineEvaluator(EvaluationContext ctxt, SparkPipelineTranslator translator) {
    super(translator);
    this.ctxt = ctxt;
  }

  @Override
  protected <PT extends PTransform<? super PInput, POutput>> void doVisitTransform(TransformTreeNode
      node) {
    @SuppressWarnings("unchecked")
    PT transform = (PT) node.getTransform();
    @SuppressWarnings("unchecked")
    Class<PT> transformClass = (Class<PT>) (Class<?>) transform.getClass();
    @SuppressWarnings("unchecked") TransformEvaluator<PT> evaluator =
        (TransformEvaluator<PT>) translator.translate(transformClass);
    LOG.info("Evaluating {}", transform);
    AppliedPTransform<PInput, POutput, PT> appliedTransform =
        AppliedPTransform.of(node.getFullName(), node.getInput(), node.getOutput(), transform);
    ctxt.setCurrentTransform(appliedTransform);
    evaluator.evaluate(transform, ctxt);
    ctxt.setCurrentTransform(null);
  }
}
