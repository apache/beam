/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import java.time.Duration
import java.time.Instant
import org.gradle.BuildAdapter
import org.gradle.BuildResult
import org.gradle.api.GradleException
import org.gradle.api.invocation.Gradle


/**
 * BuildListener that throws an exception if the build was successful but took
 * longer than a given number of minutes to complete.
 */
class BuildTimer extends BuildAdapter {
  private Instant buildBegin;
  private long maxMinutes;

  public BuildTimer(long maxMinutes) {
    this.maxMinutes = maxMinutes;
    // BuildStarted is never called for user-defined listeners, so start the timer now.
    // https://github.com/gradle/gradle/issues/4315
    buildBegin = Instant.now();
  }

  @Override
  void buildFinished(BuildResult result) {
    Duration duration = Duration.between(buildBegin, Instant.now());
    float minutes = duration.toSeconds() / 60;
    if (result.failure != null) {
      return
    }
    if (minutes > maxMinutes) {
      throw new GradleException("Build took longer than $maxMinutes minutes: $minutes");
    }
  }
}
