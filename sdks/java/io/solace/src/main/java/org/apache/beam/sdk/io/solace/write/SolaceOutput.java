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
package org.apache.beam.sdk.io.solace.write;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The {@link SolaceIO.Write} transform's output return this type, containing both the successful
 * publishes ({@link #getSuccessfulPublish()}) and the failed publishes ({@link
 * #getFailedPublish()}).
 *
 * <p>The streaming writer with DIRECT messages does not return anything, and the output {@link
 * PCollection}s will be equal to null.
 */
public final class SolaceOutput implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<Solace.PublishResult> failedPublishTag;
  private final TupleTag<Solace.PublishResult> successfulPublishTag;
  private final @Nullable PCollection<Solace.PublishResult> failedPublish;
  private final @Nullable PCollection<Solace.PublishResult> successfulPublish;

  public @Nullable PCollection<Solace.PublishResult> getFailedPublish() {
    return failedPublish;
  }

  public @Nullable PCollection<Solace.PublishResult> getSuccessfulPublish() {
    return successfulPublish;
  }

  public static SolaceOutput in(
      Pipeline pipeline,
      @Nullable PCollection<Solace.PublishResult> failedPublish,
      @Nullable PCollection<Solace.PublishResult> successfulPublish) {
    return new SolaceOutput(
        pipeline,
        SolaceIO.Write.FAILED_PUBLISH_TAG,
        SolaceIO.Write.SUCCESSFUL_PUBLISH_TAG,
        failedPublish,
        successfulPublish);
  }

  private SolaceOutput(
      Pipeline pipeline,
      TupleTag<Solace.PublishResult> failedPublishTag,
      TupleTag<Solace.PublishResult> successfulPublishTag,
      @Nullable PCollection<Solace.PublishResult> failedPublish,
      @Nullable PCollection<Solace.PublishResult> successfulPublish) {
    this.pipeline = pipeline;
    this.failedPublishTag = failedPublishTag;
    this.successfulPublishTag = successfulPublishTag;
    this.failedPublish = failedPublish;
    this.successfulPublish = successfulPublish;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> builder = ImmutableMap.<TupleTag<?>, PValue>builder();

    if (failedPublish != null) {
      builder.put(failedPublishTag, failedPublish);
    }

    if (successfulPublish != null) {
      builder.put(successfulPublishTag, successfulPublish);
    }

    return builder.build();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
