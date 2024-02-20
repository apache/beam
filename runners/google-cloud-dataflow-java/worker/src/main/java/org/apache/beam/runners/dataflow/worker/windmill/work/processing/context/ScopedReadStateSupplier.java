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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import java.io.Closeable;
import java.util.Optional;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;

class ScopedReadStateSupplier implements Supplier<Closeable> {
  private static final String READ_STATE_NAME = "windmill-read";
  private final ExecutionStateTracker.ExecutionState readState;
  private final @Nullable ExecutionStateTracker stateTracker;

  ScopedReadStateSupplier(
      DataflowOperationContext operationContext, @Nullable ExecutionStateTracker stateTracker) {
    this.readState = operationContext.newExecutionState(READ_STATE_NAME);
    this.stateTracker = stateTracker;
  }

  @Override
  public @Nullable Closeable get() {
    return Optional.ofNullable(stateTracker)
        .map(tracker -> tracker.enterState(readState))
        .orElse(null);
  }
}
