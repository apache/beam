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
   * <p>Currently only the list of errors returned from BigQuery. In the future this may contain
   * more information - e.g. how many times this insert has been retried, and for how long.
   */
  public static class Context {
    // A list of all errors corresponding to an attempted insert of a single record.
    final TableDataInsertAllResponse.InsertErrors errors;

    public TableDataInsertAllResponse.InsertErrors getInsertErrors() {
      return errors;
    }

    public Context(TableDataInsertAllResponse.InsertErrors errors) {
      this.errors = errors;
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

  /** Retry all failures except for known persistent errors. */
  public static InsertRetryPolicy retryTransientErrors() {
    return new InsertRetryPolicy() {
      @Override
      public boolean shouldRetry(Context context) {
        if (context.getInsertErrors().getErrors() != null) {
          for (ErrorProto error : context.getInsertErrors().getErrors()) {
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
