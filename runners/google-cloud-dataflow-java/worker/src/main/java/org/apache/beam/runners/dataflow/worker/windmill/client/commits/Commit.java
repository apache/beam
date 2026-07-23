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
package org.apache.beam.runners.dataflow.worker.windmill.client.commits;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.MultiKeyWorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Value class for a queued commit. */
@Internal
public class Commit {

  private final ComputationState computationState;
  private final ImmutableList<Work> workBatch;
  private final @Nullable WorkItemCommitRequest singleKeyRequest;
  private final @Nullable MultiKeyWorkItemCommitRequest multiKeyRequest;

  public static Commit create(
      WorkItemCommitRequest request, ComputationState computationState, Work work) {
    Preconditions.checkArgument(request.getSerializedSize() > 0);
    return new Commit(computationState, ImmutableList.of(work), request, null);
  }

  public static Commit createMultiKey(
      MultiKeyWorkItemCommitRequest multiKeyRequest,
      ComputationState computationState,
      ImmutableList<Work> workBatch) {
    Preconditions.checkArgument(!workBatch.isEmpty());
    return new Commit(computationState, workBatch, null, multiKeyRequest);
  }

  private Commit(
      ComputationState computationState,
      ImmutableList<Work> workBatch,
      @Nullable WorkItemCommitRequest singleKeyRequest,
      @Nullable MultiKeyWorkItemCommitRequest multiKeyRequest) {
    this.computationState = computationState;
    this.workBatch = workBatch;
    this.singleKeyRequest = singleKeyRequest;
    this.multiKeyRequest = multiKeyRequest;
  }

  public final String computationId() {
    return computationState().getComputationId();
  }

  public @Nullable WorkItemCommitRequest singleKeyRequest() {
    return singleKeyRequest;
  };

  public ComputationState computationState() {
    return computationState;
  }

  public @Nullable MultiKeyWorkItemCommitRequest multiKeyRequest() {
    return multiKeyRequest;
  }

  public ImmutableList<Work> workBatch() {
    return workBatch;
  }

  public final int getSerializedByteSize() {
    if (multiKeyRequest() != null) {
      return checkStateNotNull(multiKeyRequest()).getSerializedSize();
    }
    return checkStateNotNull(singleKeyRequest()).getSerializedSize();
  }

  @Override
  public String toString() {
    Work work = workBatch.get(0);
    return "[computationId="
        + computationId()
        + ", shardingKey="
        + work.getShardedKey()
        + ", workId="
        + work.id()
        + ", workBatchSize="
        + workBatch.size()
        + "]";
  }
}
