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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import java.util.Optional;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Class for capturing errors on IO operations on Google Cloud Healthcare APIs resources. */
@DefaultCoder(HealthcareIOErrorCoder.class)
public class HealthcareIOError<T> {
  private static final String DATA_RESOURCE = "data_resource";
  private static final String ERROR_MESSAGE = "error_message";
  private static final String STACK_TRACE = "stack_trace";
  private static final String OBSERVED_TIME = "observed_time";
  private static final String STATUS_CODE = "status_code";

  // TODO (GH ISSUE): Hard coding schema due to JavaBeanSchema error.
  static final TypeDescriptor<HealthcareIOError<String>> STRING_RESOURCE_TYPE =
      new TypeDescriptor<HealthcareIOError<String>>() {};
  static final SerializableFunction<HealthcareIOError<String>, Row> TO_ROW_FN =
      new SerializableFunction<HealthcareIOError<String>, Row>() {
        @Override
        public Row apply(HealthcareIOError<String> input) {
          HealthcareIOError<String> safeInput = checkStateNotNull(input);
          return Row.withSchema(SCHEMA_FOR_STRING_RESOURCE_TYPE)
              .withFieldValue(DATA_RESOURCE, safeInput.getDataResource())
              .withFieldValue(ERROR_MESSAGE, safeInput.getErrorMessage())
              .withFieldValue(STACK_TRACE, safeInput.getStackTrace())
              .withFieldValue(OBSERVED_TIME, safeInput.getObservedTime())
              .withFieldValue(STATUS_CODE, safeInput.getStatusCode())
              .build();
        }
      };

  static final SerializableFunction<Row, HealthcareIOError<String>> FROM_ROW_FN =
      new SerializableFunction<Row, HealthcareIOError<String>>() {
        @Override
        public HealthcareIOError<String> apply(Row input) {
          Row safeInput = checkStateNotNull(input);
          HealthcareIOError<String> result =
              new HealthcareIOError<String>(
                  checkStateNotNull(safeInput.getString(DATA_RESOURCE)),
                  checkStateNotNull(safeInput.getString(ERROR_MESSAGE)),
                  checkStateNotNull(safeInput.getString(STACK_TRACE)),
                  checkStateNotNull(safeInput.getDateTime(OBSERVED_TIME)).toInstant(),
                  checkStateNotNull(safeInput.getInt32(STATUS_CODE)));

          return result;
        }
      };
  static Schema SCHEMA_FOR_STRING_RESOURCE_TYPE =
      Schema.of(
          Field.of(DATA_RESOURCE, FieldType.STRING),
          Field.of(ERROR_MESSAGE, FieldType.STRING),
          Field.of(STACK_TRACE, FieldType.STRING),
          Field.of(OBSERVED_TIME, FieldType.DATETIME),
          Field.of(STATUS_CODE, FieldType.INT32));
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
    String msg = Optional.ofNullable(error.getMessage()).orElse("");
    String stackTrace = Throwables.getStackTraceAsString(error);
    Integer statusCode = null;

    if (error instanceof com.google.api.client.googleapis.json.GoogleJsonResponseException) {
      statusCode = ((GoogleJsonResponseException) error).getStatusCode();
    } else if (error instanceof HealthcareHttpException) {
      statusCode = ((HealthcareHttpException) error).getStatusCode();
    }
    return new HealthcareIOError<>(dataResource, msg, stackTrace, null, statusCode);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof HealthcareIOError)) {
      return false;
    }
    HealthcareIOError<?> that = (HealthcareIOError<?>) o;
    return statusCode == that.statusCode
        && Objects.equal(dataResource, that.dataResource)
        && Objects.equal(errorMessage, that.errorMessage)
        && Objects.equal(stackTrace, that.stackTrace)
        && Objects.equal(observedTime, that.observedTime);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataResource, errorMessage, stackTrace, observedTime, statusCode);
  }
}
