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

import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.ReaderCache.ReaderInvalidator;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache.ComputationStateFetcher;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache.PerComputationStateCacheFetcher;
import org.apache.beam.sdk.annotations.Internal;

@Internal
public final class CommitCompleter implements Consumer<CompleteCommit> {
  private final ReaderInvalidator readerInvalidator;
  private final PerComputationStateCacheFetcher perComputationStateCacheFetcher;
  private final ComputationStateFetcher computationStateFetcher;

  public CommitCompleter(
      ReaderInvalidator readerInvalidator,
      PerComputationStateCacheFetcher perComputationStateCacheFetcher,
      ComputationStateFetcher computationStateFetcher) {
    this.readerInvalidator = readerInvalidator;
    this.perComputationStateCacheFetcher = perComputationStateCacheFetcher;
    this.computationStateFetcher = computationStateFetcher;
  }

  @Override
  public void accept(CompleteCommit completeCommit) {
    if (completeCommit.status() != Windmill.CommitStatus.OK) {
      readerInvalidator.invalidateReader(
          WindmillComputationKey.create(
              completeCommit.computationId(), completeCommit.shardedKey()));
      perComputationStateCacheFetcher
          .forComputation(completeCommit.computationId())
          .invalidate(completeCommit.shardedKey());
    }

    computationStateFetcher
        .fetch(completeCommit.computationId())
        .ifPresent(
            state ->
                state.completeWorkAndScheduleNextWorkForKey(
                    completeCommit.shardedKey(), completeCommit.workId()));
  }
}
