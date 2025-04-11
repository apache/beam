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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import java.io.Serializable;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A retry policy for streaming BigQuery inserts.
 *
 * <p>This retry policy currently applies only to per-element errors within successful (200 OK)
 * BigQuery responses. Non-200 responses (e.g., 400 Bad Request, 500 Internal Server Error) will
 * result in a {@code RuntimeException} and bundle failure. The subsequent handling of the failed
 * bundle (e.g., retry or final failure) is determined by the specific Runner's fault tolerance
 * mechanisms.
 *
 * @see org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl#insertAll(
 *     TableReference, java.util.List, java.util.List, BackOff,
 *     org.apache.beam.sdk.util.FluentBackoff, Sleeper,
 *     org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy, java.util.List,
 *     org.apache.beam.sdk.io.gcp.bigquery.ErrorContainer, boolean, boolean, boolean,
 *     java.util.List)
 */
public abstract class InsertRetryPolicy implements Serializable {
  /**
   * Contains information about a failed insert.
   *
   * <p>Contains the list of errors returned from BigQuery for per-element failures within a
   * successful (200 OK) response, and an optional exception for non-successful (non-200) HTTP
   * responses. In the future, this may include additional details, such as retry attempts or
   * elapsed time.
   */
  public static class Context {
    // A list of all errors corresponding to an attempted insert of a single record.
    final TableDataInsertAllResponse.@Nullable InsertErrors errors;
    // Exception thrown for non-successful (non-200) HTTP responses, if applicable.
    final @Nullable Throwable exception;

    public TableDataInsertAllResponse.@Nullable InsertErrors getInsertErrors() {
      return errors;
    }

    public @Nullable Throwable getException() {
      return exception;
    }

    // Constructor for per-element errors (existing behavior)
    public Context(TableDataInsertAllResponse.@Nullable InsertErrors errors) {
      this(errors, null);
    }

    // Constructor for both per-element errors and exceptions
    public Context(
        TableDataInsertAllResponse.@Nullable InsertErrors errors, @Nullable Throwable exception) {
      this.errors = errors;
      this.exception = exception;
    }
  }

  // A list of known persistent errors for which retrying never helps.
  static final Set<String> PERSISTENT_ERRORS =
      ImmutableSet.of("invalid", "invalidQuery", "notImplemented", "row-too-large", "parseError");

  /** Return true if this failure should be retried. */
  public abstract boolean shouldRetry(Context context);

  /** Never retry any failures. */
  public static InsertRetryPolicy neverRetry() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        return false;
      }
    };
  }

  /** Always retry all failures. */
  public static InsertRetryPolicy alwaysRetry() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        return true;
      }
    };
  }

  /** Retry all failures except for known persistent errors or non-retryable exceptions. */
  public static InsertRetryPolicy retryTransientErrors() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        // Check for non-200 response exceptions first
        if (context.getException() != null) {
          // For now, assume non-200 responses are non-retryable unless specified otherwise
          return false; // Could be refined later based on exception type (e.g., 503 vs 400)
        }
        // Existing logic for per-element errors
        TableDataInsertAllResponse.InsertErrors insertErrors = context.getInsertErrors();
        if (insertErrors != null && insertErrors.getErrors() != null) {
          for (ErrorProto error : insertErrors.getErrors()) {
            if (error.getReason() != null && PERSISTENT_ERRORS.contains(error.getReason())) {
              return false;
            }
          }
        }
        return true;
      }
    };
  }
}
