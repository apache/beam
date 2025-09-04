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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import java.io.Serializable;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A retry policy for streaming BigQuery inserts.
 *
 * <p>This retry policy applies to both per-element errors within successful (200 OK) BigQuery
 * responses and non-200 HTTP responses (e.g., 400 Bad Request, 429 Too Many Requests, 500 Internal
 * Server Error). Non-200 responses are handled by passing a {@code GoogleJsonResponseException} to
 * the retry policy, allowing per-row retry decisions based on HTTP status codes. Rows that should
 * not be retried are sent to the Dead Letter Queue (DLQ).
 *
 * @see org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.DatasetServiceImpl#insertAll
 */
public abstract class InsertRetryPolicy implements Serializable {
  /**
   * Contains information about a failed insert.
   *
   * <p>Includes either per-row errors from a 200 OK response or a {@code
   * GoogleJsonResponseException} for non-200 HTTP responses. Future extensions may include retry
   * attempt counts or durations.
   */
  public static class Context {
    private final TableDataInsertAllResponse.@Nullable InsertErrors errors;
    private final @Nullable GoogleJsonResponseException exception;

    public Context(TableDataInsertAllResponse.@Nullable InsertErrors errors) {
      this(errors, null);
    }

    public Context(
            TableDataInsertAllResponse.@Nullable InsertErrors errors,
            @Nullable GoogleJsonResponseException exception) {
      this.errors = errors;
      this.exception = exception;
    }

    public TableDataInsertAllResponse.@Nullable InsertErrors getInsertErrors() {
      return errors;
    }

    public @Nullable GoogleJsonResponseException getHttpException() {
      return exception;
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

  /** Always retry all failures, including transient HTTP errors (429, 503). */
  public static InsertRetryPolicy alwaysRetry() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        GoogleJsonResponseException httpException = context.getHttpException();
        if (httpException != null) {
          int statusCode = httpException.getStatusCode();
          return statusCode == 429 || statusCode == 503;
        }
        TableDataInsertAllResponse.InsertErrors insertErrors = context.getInsertErrors();
        return insertErrors != null;
      }
    };
  }

  /** Retry all failures except for known persistent errors and non-retryable HTTP errors. */
  public static InsertRetryPolicy retryTransientErrors() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        GoogleJsonResponseException httpException = context.getHttpException();
        if (httpException != null) {
          int statusCode = httpException.getStatusCode();
          return statusCode == 429 || statusCode == 503;
        }
        TableDataInsertAllResponse.InsertErrors insertErrors = context.getInsertErrors();
        if (insertErrors != null && insertErrors.getErrors() != null) {
          for (ErrorProto error : insertErrors.getErrors()) {
            String reason = error.getReason();
            if (reason != null && PERSISTENT_ERRORS.contains(reason)) {
              return false;
            }
          }
          return true;
        }
        return false;
      }
    };
  }
}