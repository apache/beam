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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Class for capturing errors on IO operations on Google Cloud Healthcare APIs resources. */
@DefaultCoder(HealthcareIOErrorCoder.class)
public class HealthcareIOError<T> {
  private T dataResource;
  private String errorMessage;
  private String stackTrace;
  private Instant observedTime;
  private int statusCode;

  HealthcareIOError(
      T dataResource,
      String errorMessage,
      String stackTrace,
      @Nullable Instant observedTime,
      @Nullable Integer statusCode) {
    this.dataResource = dataResource;
    this.errorMessage = errorMessage;
    this.stackTrace = stackTrace;
    if (statusCode != null) {
      this.statusCode = statusCode;
    }
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

  public Integer getStatusCode() {
    return statusCode;
  }

  static <T> HealthcareIOError<T> of(T dataResource, Exception error) {
    String msg = error.getMessage();
    String stackTrace = Throwables.getStackTraceAsString(error);
    Integer statusCode = null;

    if (error instanceof com.google.api.client.googleapis.json.GoogleJsonResponseException) {
      statusCode = ((GoogleJsonResponseException) error).getStatusCode();
    } else if (error instanceof HealthcareHttpException) {
      statusCode = ((HealthcareHttpException) error).getStatusCode();
    }
    return new HealthcareIOError<>(dataResource, msg, stackTrace, null, statusCode);
  }
}
