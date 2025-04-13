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
 *
 * EDIT BY: NopAngel | Angel Nieto (FORK)
 *
 */
package org.apache.beam.sdk;

import java.io.IOException;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * Represents the result of {@link Pipeline#run()}.
 * Acts as a handle for managing and monitoring pipeline execution.
 */
public interface PipelineResult {

    /**
     * Retrieves the current execution state of the pipeline.
     *
     * @return the {@link State} representing the pipeline's status.
     */
    State getState();

    /**
     * Cancels the pipeline execution.
     *
     * @return the final {@link State} after cancellation.
     * @throws IOException if an error occurs while cancelling the pipeline.
     * @throws UnsupportedOperationException if cancellation is not supported by the runner.
     */
    State cancel() throws IOException;

    /**
     * Waits for the pipeline to finish within a specified timeout duration.
     *
     * @param duration Duration to wait. Use less than 1 ms for infinite wait.
     * @return The final {@link State} of the pipeline or {@code null} on timeout.
     * @throws UnsupportedOperationException if the runner does not support waiting with a timeout.
     */
    State waitUntilFinish(Duration duration);

    /**
     * Waits for the pipeline to finish and returns its final state.
     *
     * @return The final {@link State} of the pipeline.
     * @throws UnsupportedOperationException if the runner does not support waiting for completion.
     */
    State waitUntilFinish();

    /**
     * Provides access to metrics for the executed pipeline.
     *
     * @return {@link MetricResults} for the pipeline.
     * @throws UnsupportedOperationException if metric retrieval is not supported by the runner.
     */
    MetricResults metrics();

    /**
     * Enum representing the possible states of a pipeline job.
     */
    enum State {
        /** State is unknown or unspecified. */
        UNKNOWN(false, false),

        /** Pipeline is paused or not yet started. */
        STOPPED(false, false),

        /** Pipeline is currently running. */
        RUNNING(false, false),

        /** Pipeline completed successfully. */
        DONE(true, false),

        /** Pipeline execution failed. */
        FAILED(true, false),

        /** Pipeline was explicitly cancelled. */
        CANCELLED(true, false),

        /** Pipeline has been updated. */
        UPDATED(true, true),

        /** State reported by the runner is unrecognized. */
        UNRECOGNIZED(false, false);

        private final boolean terminal;
        private final boolean hasReplacement;

        State(boolean terminal, boolean hasReplacement) {
            this.terminal = terminal;
            this.hasReplacement = hasReplacement;
        }

        /**
         * Indicates if the job state is terminal.
         *
         * @return {@code true} if the pipeline can no longer complete work.
         */
        public boolean isTerminal() {
            return terminal;
        }

        /**
         * Indicates if a replacement job exists for the current state.
         *
         * @return {@code true} if a replacement job exists.
         */
        public boolean hasReplacementJob() {
            return hasReplacement;
        }
    }
}
