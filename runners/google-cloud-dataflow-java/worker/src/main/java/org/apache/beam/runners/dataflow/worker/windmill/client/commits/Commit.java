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
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** Value class for a queued commit. */
@Internal
@AutoValue
public abstract class Commit {

  public static Commit create(
      WorkItemCommitRequest request, ComputationState computationState, Work work) {
    Preconditions.checkArgument(request.getSerializedSize() > 0);
    return new AutoValue_Commit(request, computationState, work);
  }

  public final String computationId() {
    return computationState().getComputationId();
  }

  public abstract WorkItemCommitRequest request();

  public abstract ComputationState computationState();

  public abstract Work work();

  public final int getSize() {
    return request().getSerializedSize();
  }
}
