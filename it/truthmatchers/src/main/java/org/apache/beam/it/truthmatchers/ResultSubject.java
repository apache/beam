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
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;

/**
 * Subject that has assertion operations for {@link Result}, which is the end result of a pipeline
 * run.
 */
public final class ResultSubject extends Subject {

  private final Result actual;

  private ResultSubject(FailureMetadata metadata, Result actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public static Factory<ResultSubject, Result> result() {
    return ResultSubject::new;
  }

  /**
   * Check if the launch meets expected conditions given to a `waitForCondition...` method such as
   * {@link PipelineOperator#waitForCondition(Config, Supplier...)} or {@link
   * PipelineOperator#waitForConditionAndFinish(Config, Supplier)}.
   */
  public void meetsConditions() {
    check("check if meets all conditions").that(actual).isEqualTo(Result.CONDITION_MET);
  }

  /**
   * Check if the launch finished (is in a successful final state). It doesn't mean that the jobs
   * meet any conditions. For any `waitForCondition...` methods, checking if the launch {@link
   * #meetsConditions()} is more appropriate for testing purposes.
   */
  public void isLaunchFinished() {
    check("check if result is finished").that(actual).isEqualTo(Result.LAUNCH_FINISHED);
  }

  /** Check if the launch finished with a timeout. */
  public void hasTimedOut() {
    check("check if result timed out").that(actual).isEqualTo(Result.TIMEOUT);
  }

  /** Check if the subject finished with a failed state. */
  public void hasFailed() {
    check("check if result timed out").that(actual).isEqualTo(Result.LAUNCH_FAILED);
  }
}
