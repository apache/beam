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

import com.google.auto.value.AutoValue;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Value class for a queued commit. */
@Internal
@AutoValue
public abstract class Commit {

  public static Commit create(
      WorkItemCommitRequest request, ComputationState computationState, Work work) {
    Preconditions.checkArgument(request.getSerializedSize() > 0);
    return new AutoValue_Commit(
        request, computationState, work, Optional.empty(), ImmutableList.of(work));
  }

  public static Commit createMultiKey(
      Windmill.MultiKeyWorkItemCommitRequest multiKeyRequest,
      ComputationState computationState,
      ImmutableList<Work> workBatch) {
    return new AutoValue_Commit(
        WorkItemCommitRequest.getDefaultInstance(),
        computationState,
        workBatch.get(0),
        Optional.of(multiKeyRequest),
        workBatch);
  }

  public final String computationId() {
    return computationState().getComputationId();
  }

  public abstract WorkItemCommitRequest request();

  public abstract ComputationState computationState();

  public abstract Work work();

  public abstract Optional<Windmill.MultiKeyWorkItemCommitRequest> multiKeyRequest();

  public abstract ImmutableList<Work> workBatch();

  public final boolean isFailed() {
    for (Work w : workBatch()) {
      if (w.isFailed()) {
        return true;
      }
    }
    return false;
  }

  public final int getSize() {
    if (multiKeyRequest().isPresent()) {
      return multiKeyRequest().get().getSerializedSize();
    }
    return request().getSerializedSize();
  }
}
