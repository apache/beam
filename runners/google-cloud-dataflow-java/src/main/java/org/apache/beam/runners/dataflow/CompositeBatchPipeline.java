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
package org.apache.beam.runners.dataflow;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * A composite {@link Pipeline} that combines multiple independent test {@link Pipeline} graphs so
 * they can be translated and executed together in a single Dataflow job.
 */
class CompositeBatchPipeline extends Pipeline {

  private final List<Pipeline> memberPipelines;

  CompositeBatchPipeline(PipelineOptions batchOptions, List<Pipeline> memberPipelines) {
    super(batchOptions);
    this.memberPipelines = ImmutableList.copyOf(memberPipelines);
  }

  List<Pipeline> getMemberPipelines() {
    return memberPipelines;
  }

  @Override
  public void traverseTopologically(PipelineVisitor visitor) {
    for (Pipeline member : memberPipelines) {
      member.traverseTopologically(visitor);
    }
  }

  @Override
  public void replaceAll(List<PTransformOverride> overrides) {
    for (Pipeline member : memberPipelines) {
      member.replaceAll(overrides);
    }
  }
}
