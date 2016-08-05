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

import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;

import org.joda.time.Duration;

import java.io.IOException;

/**
 * Result of {@link Pipeline#run()}.
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
   * Waits until the pipeline finishes and returns the final status.
   * It times out after the given duration.
   *
   * @param duration The time to wait for the pipeline to finish.
   *     Provide a value less than 1 ms for an infinite wait.
   *
   * @return The final state of the pipeline or null on timeout.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException if the thread is interrupted.
   * @throws UnsupportedOperationException if the runner does not support cancellation.
   */
  State waitUntilFinish(Duration duration) throws IOException, InterruptedException;

  /**
   * Waits until the pipeline finishes and returns the final status.
   *
   * @return The final state of the pipeline.
   * @throws IOException If there is a persistent problem getting job
   *   information.
   * @throws InterruptedException if the thread is interrupted.
   * @throws UnsupportedOperationException if the runner does not support cancellation.
   */
  State waitUntilFinish() throws IOException, InterruptedException;

  /**
   * Retrieves the current value of the provided {@link Aggregator}.
   *
   * @param aggregator the {@link Aggregator} to retrieve values for.
   * @return the current values of the {@link Aggregator},
   * which may be empty if there are no values yet.
   * @throws AggregatorRetrievalException if the {@link Aggregator} values could not be retrieved.
   */
  <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException;

  // TODO: method to retrieve error messages.

  /** Named constants for common values for the job state. */
  public enum State {

    /** The job state could not be obtained or was not specified. */
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
    UPDATED(true, true);

    private final boolean terminal;

    private final boolean hasReplacement;

    private State(boolean terminal, boolean hasReplacement) {
      this.terminal = terminal;
      this.hasReplacement = hasReplacement;
    }

    /**
     * @return {@code true} if the job state can no longer complete work.
     */
    public final boolean isTerminal() {
      return terminal;
    }

    /**
     * @return {@code true} if this job state indicates that a replacement job exists.
     */
    public final boolean hasReplacementJob() {
      return hasReplacement;
    }
  }
}
