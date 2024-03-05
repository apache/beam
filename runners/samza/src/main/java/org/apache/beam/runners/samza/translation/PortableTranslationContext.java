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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.InputDescriptor;

/**
 * Helper that keeps the mapping from BEAM PCollection id to Samza {@link MessageStream}. It also
 * provides other context data such as input and output of a {@link
 * org.apache.beam.model.pipeline.v1.RunnerApi.PTransform}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PortableTranslationContext extends TranslationContext {
  private final Map<String, MessageStream<?>> messageStreams = new HashMap<>();
  private final JobInfo jobInfo;

  private PipelineNode.PTransformNode currentTransform;

  public PortableTranslationContext(
      StreamApplicationDescriptor appDescriptor, SamzaPipelineOptions options, JobInfo jobInfo) {
    super(appDescriptor, Collections.emptyMap(), Collections.emptySet(), options);
    this.jobInfo = jobInfo;
  }

  public <T> List<MessageStream<OpMessage<T>>> getAllInputMessageStreams(
      PipelineNode.PTransformNode transform) {
    final Collection<String> inputStreamIds = transform.getTransform().getInputsMap().values();
    return inputStreamIds.stream().map(this::<T>getMessageStreamById).collect(Collectors.toList());
  }

  public <T> MessageStream<OpMessage<T>> getOneInputMessageStream(
      PipelineNode.PTransformNode transform) {
    String id = Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
    return getMessageStreamById(id);
  }

  @SuppressWarnings("unchecked")
  public <T> MessageStream<OpMessage<T>> getMessageStreamById(String id) {
    return (MessageStream<OpMessage<T>>) messageStreams.get(id);
  }

  public String getInputId(PipelineNode.PTransformNode transform) {
    return Iterables.getOnlyElement(transform.getTransform().getInputsMap().values());
  }

  public String getOutputId(PipelineNode.PTransformNode transform) {
    return Iterables.getOnlyElement(transform.getTransform().getOutputsMap().values());
  }

  public JobInfo getJobInfo() {
    return jobInfo;
  }

  public <T> void registerMessageStream(String id, MessageStream<OpMessage<T>> stream) {
    if (messageStreams.containsKey(id)) {
      throw new IllegalArgumentException("Stream already registered for id: " + id);
    }
    messageStreams.put(id, stream);
  }

  /** Register an input stream with certain config id. */
  public <T> void registerInputMessageStream(
      String id, InputDescriptor<KV<?, OpMessage<T>>, ?> inputDescriptor) {
    registerInputMessageStreams(id, Collections.singletonList(inputDescriptor));
  }

  public <T> void registerInputMessageStreams(
      String id, List<? extends InputDescriptor<KV<?, OpMessage<T>>, ?>> inputDescriptors) {
    registerInputMessageStreams(id, inputDescriptors, this::registerMessageStream);
  }

  public void setCurrentTransform(PipelineNode.PTransformNode currentTransform) {
    this.currentTransform = currentTransform;
  }

  @Override
  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  @Override
  public String getTransformFullName() {
    return currentTransform.getTransform().getUniqueName();
  }
}
