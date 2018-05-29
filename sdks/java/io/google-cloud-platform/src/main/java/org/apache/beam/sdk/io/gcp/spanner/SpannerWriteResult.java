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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A result of {@link SpannerIO#write()} transform. Use {@link #getFailedMutations} to access
 * failed Mutations. {@link #getOutput()} can be used as a completion signal with the
 * {@link org.apache.beam.sdk.transforms.Wait} transform.
 */
public class SpannerWriteResult implements POutput {
  private final Pipeline pipeline;
  private final PCollection<Void> output;
  private final PCollection<MutationGroup> failedMutations;
  private final TupleTag<MutationGroup> failedMutationsTag;

  public SpannerWriteResult(Pipeline pipeline, PCollection<Void> output,
      PCollection<MutationGroup> failedMutations, TupleTag<MutationGroup> failedMutationsTag) {
    this.pipeline = pipeline;
    this.output = output;
    this.failedMutations = failedMutations;
    this.failedMutationsTag = failedMutationsTag;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(failedMutationsTag, failedMutations);
  }

  public PCollection<MutationGroup> getFailedMutations() {
    return failedMutations;
  }

  public PCollection<Void> getOutput() {
    return output;
  }

  @Override
  public void finishSpecifyingOutput(String transformName, PInput input,
      PTransform<?, ?> transform) {

  }
}
