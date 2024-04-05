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
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.WorkId;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * A {@link Commit} is marked as complete when it has been attempted to be committed back to
 * Streaming Engine/Appliance via {@link
 * org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub#commitWorkStream(StreamObserver)}
 * for Streaming Engine or {@link
 * org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub#commitWork(Windmill.CommitWorkRequest,
 * StreamObserver)} for Streaming Appliance.
 */
@Internal
@AutoValue
public abstract class CompleteCommit {

  public static CompleteCommit create(Commit commit, CommitStatus commitStatus) {
    return new AutoValue_CompleteCommit(
        commit.computationId(),
        ShardedKey.create(commit.request().getKey(), commit.request().getShardingKey()),
        WorkId.builder()
            .setWorkToken(commit.request().getWorkToken())
            .setCacheToken(commit.request().getCacheToken())
            .build(),
        commitStatus);
  }

  public static CompleteCommit create(
      String computationId, ShardedKey shardedKey, WorkId workId, CommitStatus status) {
    return new AutoValue_CompleteCommit(computationId, shardedKey, workId, status);
  }

  public static CompleteCommit forFailedWork(Commit commit) {
    return create(commit, CommitStatus.ABORTED);
  }

  public abstract String computationId();

  public abstract ShardedKey shardedKey();

  public abstract WorkId workId();

  public abstract CommitStatus status();
}
