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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

/**
 * Translate {@link org.apache.beam.sdk.transforms.Impulse} to a samza message stream produced by
 * {@link
 * org.apache.beam.runners.samza.translation.SamzaImpulseSystemFactory.SamzaImpulseSystemConsumer}.
 */
public class ImpulseTranslator implements TransformTranslator, TransformConfigGenerator {
  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    final String outputId = ctx.getOutputId(transform);
    ctx.registerInputMessageStream(outputId);
  }

  @Override
  public Map<String, String> createPortableConfig(PipelineNode.PTransformNode transform) {
    final String id = Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());

    final Map<String, String> config = new HashMap<>();
    final String systemPrefix = "systems." + id;
    final String streamPrefix = "streams." + id;

    config.put(systemPrefix + ".samza.factory", SamzaImpulseSystemFactory.class.getName());
    config.put(streamPrefix + ".samza.system", id);

    return config;
  }
}
