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

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.CloseableStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.sdk.annotations.Internal;

/** Utility class for creating {@link WorkCommitter} implementations. */
@Internal
public final class WorkCommitters {
  public static WorkCommitter createApplianceWorkCommitter(
      Consumer<Windmill.CommitWorkRequest> commitWork,
      int numCommitWorkers,
      Supplier<Boolean> shouldCommitWork,
      CountDownLatch ready) {
    return StreamingApplianceWorkCommitter.create(
        commitWork, numCommitWorkers, shouldCommitWork, ready);
  }

  public static WorkCommitter createStreamingEngineWorkCommitter(
      Supplier<CloseableStream<WindmillStream.CommitWorkStream>> commitWorkStreamFactory,
      int numCommitSenders,
      Supplier<Boolean> shouldCommitWork,
      Consumer<Commit> onFailedCommit,
      Consumer<CompleteCommit> onCommitComplete,
      CountDownLatch ready) {
    return StreamingEngineWorkCommitter.create(
        commitWorkStreamFactory,
        numCommitSenders,
        shouldCommitWork,
        onFailedCommit,
        onCommitComplete,
        ready);
  }
}
