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
package org.apache.beam.sdk.io.gcp.healthcare;

import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;

/** Class for capturing errors on IO operations on Google Cloud Healthcare APIs resources. */
public class HealthcareIOError<T> {
  private T dataResource;
  private String errorMessage;
  private String stackTrace;
  private Instant observedTime;

  HealthcareIOError(
      T dataResource, String errorMessage, String stackTrace, @Nullable Instant observedTime) {
    this.dataResource = dataResource;
    this.errorMessage = errorMessage;
    this.stackTrace = stackTrace;
    if (observedTime != null) {
      this.observedTime = observedTime;
    } else {
      this.observedTime = Instant.now();
    }
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public Instant getObservedTime() {
    return observedTime;
  }

  public T getDataResource() {
    return dataResource;
  }

  static <T> HealthcareIOError<T> of(T dataResource, Exception error) {
    String msg = error.getMessage();
    String stackTrace = Throwables.getStackTraceAsString(error);
    return new HealthcareIOError<>(dataResource, msg, stackTrace, null);
  }
}
