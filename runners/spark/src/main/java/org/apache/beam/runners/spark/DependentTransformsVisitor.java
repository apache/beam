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
package org.apache.beam.runners.spark;

import java.util.Map;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Traverses the pipeline to populate information on how many {@link
 * org.apache.beam.sdk.transforms.PTransform}s do consume / depends on each {@link PCollection} in
 * the pipeline.
 */
class DependentTransformsVisitor extends SparkRunner.Evaluator {

  DependentTransformsVisitor(
      SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
    super(translator, evaluationContext);
  }

  @Override
  public void doVisitTransform(TransformHierarchy.Node node) {

    for (Map.Entry<TupleTag<?>, PCollection<?>> entry : node.getInputs().entrySet()) {
      ctxt.reportPCollectionConsumed(entry.getValue());
    }

    for (PCollection<?> pOut : node.getOutputs().values()) {
      ctxt.reportPCollectionProduced(pOut);
    }
  }
}
