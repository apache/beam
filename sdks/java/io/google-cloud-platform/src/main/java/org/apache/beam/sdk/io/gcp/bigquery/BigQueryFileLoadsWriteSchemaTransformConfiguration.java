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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Configuration for writing to BigQuery.
 *
 * <p>This class is meant to be used with {@link BigQueryFileLoadsWriteSchemaTransformProvider}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryFileLoadsWriteSchemaTransformConfiguration {

  /** Instantiates a {@link BigQueryFileLoadsWriteSchemaTransformConfiguration.Builder}. */
  public static Builder builder() {
    return new AutoValue_BigQueryFileLoadsWriteSchemaTransformConfiguration.Builder();
  }

  /**
   * Writes to the given table specification. See {@link BigQueryIO.Write#to(String)}} for the
   * expected format.
   */
  public abstract String getTableSpec();

  /** Specifies whether the table should be created if it does not exist. */
  public abstract String getCreateDisposition();

  /** Specifies what to do with existing data in the table, in case the table already exists. */
  public abstract String getWriteDisposition();

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Writes to the given table specification. See {@link BigQueryIO.Write#to(String)}} for the
     * expected format.
     */
    public abstract Builder setTableSpec(String value);

    /** Specifies whether the table should be created if it does not exist. */
    public abstract Builder setCreateDisposition(String value);

    /** Specifies what to do with existing data in the table, in case the table already exists. */
    public abstract Builder setWriteDisposition(String value);

    /** Builds the {@link BigQueryFileLoadsWriteSchemaTransformConfiguration} configuration. */
    public abstract BigQueryFileLoadsWriteSchemaTransformConfiguration build();
  }
}
