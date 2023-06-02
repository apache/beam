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

import com.google.auto.value.AutoValue;

/**
 * This class indicates how to apply a row update to BigQuery. A sequence number must always be
 * supplied to order the updates. Incorrect sequence numbers will result in unexpected state in the
 * BigQuery table.
 */
@AutoValue
public abstract class RowMutationInformation {
  public enum MutationType {
    // Upsert the row based on the primary key. Either inserts or replaces an existing row. This is
    // an idempotent
    // operation - multiple updates will simply overwrite the same row.
    UPSERT,
    // Delete the row with the matching primary key.
    DELETE
  }

  public abstract MutationType getMutationType();

  // The sequence number used
  public abstract long getSequenceNumber();

  public static RowMutationInformation of(MutationType mutationType, long sequenceNumber) {
    return new AutoValue_RowMutationInformation(mutationType, sequenceNumber);
  }
}
