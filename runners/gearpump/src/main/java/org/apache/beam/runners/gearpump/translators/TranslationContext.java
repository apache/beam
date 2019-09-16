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
package org.apache.beam.runners.gearpump.translators;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.dsl.javaapi.JavaStream;
import io.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import io.gearpump.streaming.source.DataSource;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Maintains context data for {@link TransformTranslator}s. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TranslationContext {

  private final JavaStreamApp streamApp;
  private final GearpumpPipelineOptions pipelineOptions;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final Map<PValue, JavaStream<?>> streams = new HashMap<>();

  public TranslationContext(JavaStreamApp streamApp, GearpumpPipelineOptions pipelineOptions) {
    this.streamApp = streamApp;
    this.pipelineOptions = pipelineOptions;
  }

  public void setCurrentTransform(TransformHierarchy.Node treeNode, Pipeline pipeline) {
    this.currentTransform = treeNode.toAppliedPTransform(pipeline);
  }

  public GearpumpPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public <InputT> JavaStream<InputT> getInputStream(PValue input) {
    return (JavaStream<InputT>) streams.get(input);
  }

  public <OutputT> void setOutputStream(PValue output, JavaStream<OutputT> outputStream) {
    if (!streams.containsKey(output)) {
      streams.put(output, outputStream);
    } else {
      throw new RuntimeException("set stream for duplicated output " + output);
    }
  }

  public Map<TupleTag<?>, PValue> getInputs() {
    return getCurrentTransform().getInputs();
  }

  public PValue getInput() {
    return Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(getCurrentTransform()));
  }

  public Map<TupleTag<?>, PValue> getOutputs() {
    return getCurrentTransform().getOutputs();
  }

  public PValue getOutput() {
    return Iterables.getOnlyElement(getOutputs().values());
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    checkArgument(currentTransform != null, "current transform not set");
    return currentTransform;
  }

  public <T> JavaStream<T> getSourceStream(DataSource dataSource) {
    return streamApp.source(
        dataSource, pipelineOptions.getParallelism(), UserConfig.empty(), "source");
  }
}
