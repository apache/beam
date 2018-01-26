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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.adapter.BoundedSourceSystem;
import org.apache.beam.runners.samza.adapter.UnboundedSourceSystem;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;

/**
 * Builder class to generate configs for BEAM samza runner during runtime.
 */
public class ConfigBuilder extends Pipeline.PipelineVisitor.Defaults {
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Map<PValue, String> idMap;
  private final Map<String, String> config = new HashMap<>();
  private final Pipeline pipeline;
  private boolean foundSource = false;

  public static Config buildConfig(Pipeline pipeline,
                                   SamzaPipelineOptions options,
                                   Map<PValue, String> idMap) {
    try {
      final Map<String, String> config = new HashMap<>(options.getSamzaConfig());
      config.put(JobConfig.JOB_NAME(), options.getJobName());
      config.put("beamPipelineOptions",
          Base64Serializer.serializeUnchecked(new SerializablePipelineOptions(options)));
      // TODO: remove after SAMZA-1531 is resolved
      config.put(ApplicationConfig.APP_RUN_ID, String.valueOf(System.currentTimeMillis()) + "-"
          // use the most significant bits in UUID (8 digits) to avoid collision
          + UUID.randomUUID().toString().substring(0, 8));

      final ConfigBuilder builder = new ConfigBuilder(idMap, pipeline);
      pipeline.traverseTopologically(builder);
      builder.checkFoundSource();
      config.putAll(builder.getConfig());
      return new MapConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigBuilder(Map<PValue, String> idMap,
                        Pipeline pipeline) {
    this.idMap = idMap;
    this.pipeline = pipeline;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (node.getTransform() instanceof Read.Bounded) {
      foundSource = true;
      processReadBounded(node, (Read.Bounded<?>) node.getTransform());
    } else if (node.getTransform() instanceof Read.Unbounded) {
      foundSource = true;
      processReadUnbounded(node, (Read.Unbounded<?>) node.getTransform());
    }
  }

  private <T> void processReadBounded(TransformHierarchy.Node node, Read.Bounded<T> transform) {
    final String id = getId(Iterables.getOnlyElement(node.getOutputs().values()));
    final BoundedSource<T> source = transform.getSource();

    @SuppressWarnings("unchecked")
    final PCollection<T> output =
        (PCollection<T>) Iterables.getOnlyElement(
            node.toAppliedPTransform(pipeline).getOutputs().values());

    @SuppressWarnings("unchecked")
    final WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
        (WindowingStrategy<T, ? extends BoundedWindow>) output.getWindowingStrategy();

    final Coder<WindowedValue<T>> coder =
        WindowedValue.FullWindowedValueCoder.of(
            source.getDefaultOutputCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    config.putAll(BoundedSourceSystem.createConfigFor(id, source, coder, node.getFullName()));
  }

  private <T> void processReadUnbounded(TransformHierarchy.Node node, Read.Unbounded<T> transform) {
    final String id = getId(Iterables.getOnlyElement(node.getOutputs().values()));
    final UnboundedSource<T, ?> source = transform.getSource();

    @SuppressWarnings("unchecked")
    final PCollection<T> output =
        (PCollection<T>) Iterables.getOnlyElement(
            node.toAppliedPTransform(pipeline).getOutputs().values());

    @SuppressWarnings("unchecked")
    final WindowingStrategy<T, ? extends BoundedWindow> windowingStrategy =
        (WindowingStrategy<T, ? extends BoundedWindow>) output.getWindowingStrategy();

    final Coder<WindowedValue<T>> coder =
        WindowedValue.FullWindowedValueCoder.of(
            source.getDefaultOutputCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    config.putAll(UnboundedSourceSystem.createConfigFor(id, source, coder, node.getFullName()));
  }

  private String getId(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalStateException(String.format("Could not find id for pvalue: %s", pvalue));
    }
    return id;
  }

  private void checkFoundSource() {
    if (!foundSource) {
      throw new IllegalStateException("Could not find any sources in pipeline!");
    }
  }

  private Map<String, String> getConfig() {
    return config;
  }

  private String writeValueAsJsonString(Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
