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

import com.google.auto.service.AutoService;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Translates Reshuffle transform into Samza's native partitionBy operator, which will partition
 * each incoming message by the key into a Task corresponding to that key.
 */
public class RedistributeByKeyTranslator<K, V>
    implements TransformTranslator<PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>>> {

  private final ReshuffleTranslator<K, V, V> reshuffleTranslator =
      new ReshuffleTranslator<>("rdstr-");

  @Override
  public void translate(
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    reshuffleTranslator.translate(transform, node, ctx);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    reshuffleTranslator.translatePortable(transform, pipeline, ctx);
  }

  /** Predicate to determine whether a URN is a Samza native transform. */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsSamzaNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return false;
    }
  }
}
