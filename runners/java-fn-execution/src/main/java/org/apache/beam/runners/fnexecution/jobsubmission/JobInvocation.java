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
package org.apache.beam.runners.fnexecution.jobsubmission;

import java.util.function.Consumer;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;

/** Internal representation of a Job which has been invoked (prepared and run) by a client. */
public interface JobInvocation {

  /** Start the job. */
  void start();

  /** @return Unique identifier for the job invocation. */
  String getId();

  /** Cancel the job. */
  void cancel();

  /** Retrieve the job's current state. */
  JobState.Enum getState();

  /** Listen for job state changes with a {@link Consumer}. */
  void addStateListener(Consumer<Enum> stateStreamObserver);

  /** Listen for job messages with a {@link Consumer}. */
  void addMessageListener(Consumer<JobMessage> messageStreamObserver);

  static Boolean isTerminated(Enum state) {
    switch (state) {
      case DONE:
      case FAILED:
      case CANCELLED:
      case DRAINED:
        return true;
      default:
        return false;
    }
  }
}
