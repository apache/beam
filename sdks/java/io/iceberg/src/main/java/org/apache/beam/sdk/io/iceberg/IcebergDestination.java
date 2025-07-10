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
package org.apache.beam.sdk.io.iceberg;

import com.google.auto.value.AutoValue;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
public abstract class IcebergDestination {

  /**
   * The iceberg table identifier to write data to. This is relative to the catalog, which is
   * presumed to be configured outside of this destination specification.
   */
  @Pure
  public abstract TableIdentifier getTableIdentifier();

  /** File format for created files. */
  @Pure
  public abstract FileFormat getFileFormat();

  /**
   * Metadata and constraints for creating a new table, if it must be done dynamically.
   *
   * <p>If null, dynamic table creation will fail, and this should be disallowed at the top level
   * configuration.
   */
  @Pure
  public abstract @Nullable IcebergTableCreateConfig getTableCreateConfig();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergDestination.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableIdentifier(TableIdentifier tableId);

    public abstract Builder setFileFormat(FileFormat fileFormat);

    public abstract Builder setTableCreateConfig(@Nullable IcebergTableCreateConfig createConfig);

    @Pure
    public abstract IcebergDestination build();
  }
}
