/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker.logging;

/**
 * Mapped diagnostic context for the Dataflow worker.
 */
public class DataflowWorkerLoggingMDC {
  private static final InheritableThreadLocal<String> jobId = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> stageName = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> stepName = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> workerId = new InheritableThreadLocal<>();
  private static final InheritableThreadLocal<String> workId = new InheritableThreadLocal<>();

  /**
   * Sets the Job ID of the current thread, which will be inherited by child threads.
   */
  public static void setJobId(String newJobId) {
    jobId.set(newJobId);
  }

  /**
   * Sets the Stage Name of the current thread, which will be inherited by child threads.
   */
  public static void setStageName(String newStageName) {
    stageName.set(newStageName);
  }

  /**
   * Sets the Step Name of the current thread, which will be inherited by child threads.
   */
  public static void setStepName(String newStepName) {
    stepName.set(newStepName);
  }

  /**
   * Sets the Worker ID of the current thread, which will be inherited by child threads.
   */
  public static void setWorkerId(String newWorkerId) {
    workerId.set(newWorkerId);
  }

  /**
   * Sets the Work ID of the current thread, which will be inherited by child threads.
   */
  public static void setWorkId(String newWorkId) {
    workId.set(newWorkId);
  }

  /**
   * Gets the Job ID of the current thread.
   */
  public static String getJobId() {
    return jobId.get();
  }

  /**
   * Gets the Stage Name of the current thread.
   */
  public static String getStageName() {
    return stageName.get();
  }

  /**
   * Gets the Step Name of the current thread.
   */
  public static String getStepName() {
    return stepName.get();
  }

  /**
   * Gets the Worker ID of the current thread.
   */
  public static String getWorkerId() {
    return workerId.get();
  }

  /**
   * Gets the Work ID of the current thread.
   */
  public static String getWorkId() {
    return workId.get();
  }
}
