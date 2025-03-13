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
package org.apache.beam.sdk;

import java.io.IOException;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * Result of {@link Pipeline#run()}.
 *
 * <p>This is often a job handle to an underlying data processing engine.
 */
public interface PipelineResult {

  /**
   * Retrieves the current state of the pipeline execution.
   *
   * @return the {@link State} representing the state of this pipeline.
   */
  State getState();

  /**
   * Cancels the pipeline execution.
   *
   * @throws IOException if there is a problem executing the cancel request.
   * @throws UnsupportedOperationException if the runner does not support cancellation.
   */
  State cancel() throws IOException;

  /**
   * Waits until the pipeline finishes and returns the final status. It times out after the given
   * duration.
   *
   * @param duration The time to wait for the pipeline to finish. Provide a value less than 1 ms for
   *     an infinite wait.
   * @return The final state of the pipeline or null on timeout.
   * @throws UnsupportedOperationException if the runner does not support waiting to finish with a
   *     timeout.
   */
  State waitUntilFinish(Duration duration);

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @return The final state of the pipeline.
   * @throws UnsupportedOperationException if the runner does not support waiting to finish.
   */
  State waitUntilFinish();

  // TODO: method to retrieve error messages.

  /**
   * Possible job states, for both completed and ongoing jobs.
   *
   * <p>When determining if a job is still running, consult the {@link #isTerminal()} method rather
   * than inspecting the precise state.
   */
  enum State {

    /** The job state was not specified or unknown to a runner. */
    UNKNOWN(false, false),

    /** The job has been paused, or has not yet started. */
    STOPPED(false, false),

    /** The job is currently running. */
    RUNNING(false, false),

    /** The job has successfully completed. */
    DONE(true, false),

    /** The job has failed. */
    FAILED(true, false),

    /** The job has been explicitly cancelled. */
    CANCELLED(true, false),

    /** The job has been updated. */
    UPDATED(true, true),

    /** The job state reported by a runner cannot be interpreted by the SDK. */
    UNRECOGNIZED(false, false);

    private final boolean terminal;

    private final boolean hasReplacement;

    State(boolean terminal, boolean hasReplacement) {
      this.terminal = terminal;
      this.hasReplacement = hasReplacement;
    }

    /** @return {@code true} if the job state can no longer complete work. */
    public final boolean isTerminal() {
      return terminal;
    }

    /** @return {@code true} if this job state indicates that a replacement job exists. */
    public final boolean hasReplacementJob() {
      return hasReplacement;
    }
  }

  /**
   * Returns the object to access metrics from the pipeline.
   *
   * @throws UnsupportedOperationException if the runner doesn't support retrieving metrics.
   */
  MetricResults metrics();
}
