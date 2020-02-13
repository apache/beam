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
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Portable specific samza pipeline translator. This is the entry point for translating a portable
 * pipeline
 */
public class SamzaPortablePipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPortablePipelineTranslator.class);

  private static final Map<String, TransformTranslator<?>> TRANSLATORS = loadTranslators();

  private static Map<String, TransformTranslator<?>> loadTranslators() {
    Map<String, TransformTranslator<?>> translators = new HashMap<>();
    for (SamzaTranslatorRegistrar registrar : ServiceLoader.load(SamzaTranslatorRegistrar.class)) {
      translators.putAll(registrar.getTransformTranslators());
    }
    LOG.info("{} translators loaded.", translators.size());
    return ImmutableMap.copyOf(translators);
  }

  private SamzaPortablePipelineTranslator() {}

  public static void translate(RunnerApi.Pipeline pipeline, PortableTranslationContext ctx) {
    QueryablePipeline queryablePipeline =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());

    for (PipelineNode.PTransformNode transform :
        queryablePipeline.getTopologicallyOrderedTransforms()) {
      ctx.setCurrentTransform(transform);

      LOG.info("Translating transform urn: {}", transform.getTransform().getSpec().getUrn());
      TRANSLATORS
          .get(transform.getTransform().getSpec().getUrn())
          .translatePortable(transform, queryablePipeline, ctx);

      ctx.clearCurrentTransform();
    }
  }

  public static void createConfig(
      RunnerApi.Pipeline pipeline, ConfigBuilder configBuilder, SamzaPipelineOptions options) {
    QueryablePipeline queryablePipeline =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform :
        queryablePipeline.getTopologicallyOrderedTransforms()) {
      TransformTranslator<?> translator =
          TRANSLATORS.get(transform.getTransform().getSpec().getUrn());
      if (translator instanceof TransformConfigGenerator) {
        TransformConfigGenerator configGenerator = (TransformConfigGenerator) translator;
        configBuilder.putAll(configGenerator.createPortableConfig(transform, options));
      }
    }
  }
}
