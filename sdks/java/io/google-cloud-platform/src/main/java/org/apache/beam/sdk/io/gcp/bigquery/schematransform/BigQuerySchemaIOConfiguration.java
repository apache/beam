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

package org.apache.beam.sdk.io.gcp.bigquery.schematransform;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for the {@link org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaIOProvider}.
 *
 * <p><b>Internal only:</b> This is actively being worked on and will likely change.
 * We provide no backwards compatibility guarantees, and it should not be implemented outside the
 * Beam repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQuerySchemaIOConfiguration {

  private static final String API = "bigquery";
  private static final String VERSION = "v2";

  /** Unique identifier for this configuration. **/
  public static final String IDENTIFIER =
      String.format("%s:%s", API, VERSION);

  /** Returns a Builder for a BigQuery Query Job type. **/
  public static Builder builderOfQueryType() {
    return builderOf(JobType.QUERY);
  }

  /** Returns a Builder for a BigQuery Extract Job type. **/
  public static Builder builderOfExtractType() {
    return builderOf(JobType.EXTRACT);
  }

  /** Returns a Builder for a BigQuery Load Job type. **/
  public static Builder builderOfLoadType() {
    return builderOf(JobType.LOAD);
  }

  private static Builder builderOf(JobType jobType) {
    return new AutoValue_BigQuerySchemaIOConfiguration.Builder()
        .setJobType(jobType.name());
  }

  /** Get the configuration's Job type. **/
  public abstract String getJobType();

  @AutoValue.Builder
  public static abstract class Builder {

    /** Set the configuration's Job type **/
    public abstract Builder setJobType(String value);

    /** Build the configuration. **/
    public abstract BigQuerySchemaIOConfiguration build();
  }

  /** Enumeration for possible BigQuery job types supported by this configuration. **/
  public enum JobType {
    QUERY,
    EXTRACT,
    LOAD,
  }
}
