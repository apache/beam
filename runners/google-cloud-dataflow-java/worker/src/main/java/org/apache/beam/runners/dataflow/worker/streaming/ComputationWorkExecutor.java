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
package org.apache.beam.runners.dataflow.worker.streaming;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.DataflowWorkExecutor;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to process {@link Work} by executing user DoFns for a specific computation. May be reused to
 * process future work items owned a computation.
 *
 * <p>Should only be accessed by 1 thread at a time.
 *
 * @implNote Once closed, it cannot be reused.
 */
// TODO(m-trieu): See if this can be combined/cleaned up with StreamingModeExecutionContext as the
// separation of responsibilities are unclear.
@AutoValue
@Internal
@NotThreadSafe
public abstract class ComputationWorkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ComputationWorkExecutor.class);

  public static ComputationWorkExecutor.Builder builder() {
    return new AutoValue_ComputationWorkExecutor.Builder();
  }

  public abstract DataflowWorkExecutor workExecutor();

  public abstract StreamingModeExecutionContext context();

  public abstract Optional<Coder<?>> keyCoder();

  public abstract ExecutionStateTracker executionStateTracker();

  /**
   * Executes DoFns for the Work. Blocks the calling thread until DoFn(s) have completed execution.
   */
  public final void executeWork(
      @Nullable Object key,
      Work work,
      WindmillStateReader stateReader,
      SideInputStateFetcher sideInputStateFetcher,
      Windmill.WorkItemCommitRequest.Builder outputBuilder)
      throws Exception {
    context().start(key, work, stateReader, sideInputStateFetcher, outputBuilder);
    workExecutor().execute();
  }

  /**
   * Callers should only invoke invalidate() when execution of work fails. Once closed, the instance
   * cannot be reused.
   */
  public final void invalidate() {
    context().invalidateCache();
    try {
      workExecutor().close();
    } catch (Exception e) {
      LOG.warn("Failed to close map task executor: ", e);
    }
  }

  public final long computeSourceBytesProcessed(String sourceBytesCounterName) {
    HashMap<String, ElementCounter> counters =
        ((DataflowMapTaskExecutor) workExecutor())
            .getReadOperation()
            .receivers[0]
            .getOutputCounters();

    return Optional.ofNullable(counters.get(sourceBytesCounterName))
        .map(counter -> ((OutputObjectAndByteCounter) counter).getByteCount().getAndReset())
        .orElse(0L);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setWorkExecutor(DataflowWorkExecutor workExecutor);

    public abstract Builder setContext(StreamingModeExecutionContext context);

    public abstract Builder setKeyCoder(Coder<?> keyCoder);

    public abstract Builder setExecutionStateTracker(ExecutionStateTracker executionStateTracker);

    public abstract ComputationWorkExecutor build();
  }
}
