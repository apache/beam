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
package org.apache.beam.sdk.loadtests;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.joda.time.Duration;

/** Class for detecting failures after {@link PipelineResult#waitUntilFinish(Duration)} unblocks. */
class JobFailure {

  private String cause;

  private boolean requiresCancelling;

  JobFailure(String cause, boolean requiresCancelling) {
    this.cause = cause;
    this.requiresCancelling = requiresCancelling;
  }

  static void handleFailure(
      final PipelineResult pipelineResult, final List<NamedTestResult> testResults)
      throws IOException {
    Optional<JobFailure> failure = lookForFailure(pipelineResult, testResults);

    if (failure.isPresent()) {
      JobFailure jobFailure = failure.get();

      if (jobFailure.requiresCancelling) {
        pipelineResult.cancel();
      }

      throw new RuntimeException(jobFailure.cause);
    }
  }

  private static Optional<JobFailure> lookForFailure(
      PipelineResult pipelineResult, List<NamedTestResult> testResults) {
    PipelineResult.State state = pipelineResult.getState();

    Optional<JobFailure> stateRelatedFailure = lookForInvalidState(state);

    if (stateRelatedFailure.isPresent()) {
      return stateRelatedFailure;
    } else {
      return lookForMetricResultFailure(testResults);
    }
  }

  private static Optional<JobFailure> lookForMetricResultFailure(
      List<NamedTestResult> testResults) {

    boolean isTestResultInvalid =
        testResults.stream().anyMatch(namedTestResult -> namedTestResult.getValue() == -1D);

    if (isTestResultInvalid) {
      return of(new JobFailure("Invalid test results", false));
    } else {
      return empty();
    }
  }

  private static Optional<JobFailure> lookForInvalidState(PipelineResult.State state) {
    switch (state) {
      case RUNNING:
      case UNKNOWN:
        return of(new JobFailure("Job timeout.", true));

      case CANCELLED:
      case FAILED:
      case STOPPED:
      case UPDATED:
        return of(new JobFailure(format("Invalid job state: %s.", state.toString()), false));

      default:
        return empty();
    }
  }
}
