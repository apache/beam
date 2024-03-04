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
package org.apache.beam.runners.samza.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;

/** Interface of Samza translator for BEAM {@link PTransform}. */
public interface TransformTranslator<T extends PTransform<?, ?>> {

  /** Translates the Java {@link PTransform} into Samza API. */
  default void translate(T transform, TransformHierarchy.Node node, TranslationContext ctx) {
    throw new UnsupportedOperationException(
        "Java translation is not supported for " + this.getClass().getSimpleName());
  }

  /**
   * Translates the portable {@link org.apache.beam.model.pipeline.v1.RunnerApi.PTransform} into
   * Samza API.
   */
  default void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    throw new UnsupportedOperationException(
        "Portable translation is not supported for " + this.getClass().getSimpleName());
  }
}
