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
package org.apache.beam.it.truthmatchers;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import org.apache.beam.it.common.PipelineLauncher.JobState;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;

/**
 * Subject that has assertion operations for {@link LaunchInfo}, which has the information for a
 * recently launched pipeline.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class LaunchInfoSubject extends Subject {

  private final LaunchInfo actual;

  private LaunchInfoSubject(FailureMetadata metadata, LaunchInfo actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public static Factory<LaunchInfoSubject, LaunchInfo> launchInfo() {
    return LaunchInfoSubject::new;
  }

  /**
   * Check if the subject reflects succeeded states. A successful {@link LaunchInfo} does not mean
   * that the pipeline finished and no errors happened, it just means that the job was able to get
   * itself into an active state (RUNNING, UPDATED).
   */
  public void isRunning() {
    check("check if succeeded").that(actual.state()).isIn(JobState.ACTIVE_STATES);
  }

  /**
   * Check if the subject reflects failure states. A failed {@link LaunchInfo} often means that the
   * request for launching a pipeline didn't make through validations, so the job couldn't even
   * start or do any processing.
   */
  public void failed() {
    check("check if succeeded").that(actual.state()).isIn(JobState.FAILED_STATES);
  }
}
