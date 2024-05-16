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
import java.util.Optional;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowWorkExecutor;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.sdk.coders.Coder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ExecutionState {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionState.class);

  public static ExecutionState.Builder builder() {
    return new AutoValue_ExecutionState.Builder();
  }

  public abstract DataflowWorkExecutor workExecutor();

  public abstract StreamingModeExecutionContext context();

  public abstract Optional<Coder<?>> keyCoder();

  public abstract ExecutionStateTracker executionStateTracker();

  public final void close() {
    context().invalidateCache();
    try {
      workExecutor().close();
    } catch (Exception e) {
      LOG.warn("Failed to close map task executor: ", e);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setWorkExecutor(DataflowWorkExecutor workExecutor);

    public abstract Builder setContext(StreamingModeExecutionContext context);

    public abstract Builder setKeyCoder(Coder<?> keyCoder);

    public abstract Builder setExecutionStateTracker(ExecutionStateTracker executionStateTracker);

    public abstract ExecutionState build();
  }
}
