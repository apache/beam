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
package org.apache.beam.sdk.io.iceberg.maintenance;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;

/**
 * A file considered by a table maintenance operation.
 *
 * <p>This is the element type exchanged between maintenance transforms. A file is either {@link
 * #getReachable() reachable} from table state that must be preserved, or a candidate for deletion.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
@Internal
public abstract class FileInfo {

  /** The file location, exactly as recorded in table metadata or listed from storage. */
  @SchemaFieldNumber("0")
  public abstract String getPath();

  /** The role of the file in the table. */
  @SchemaFieldNumber("1")
  public abstract FileCategory getCategory();

  /**
   * Whether the file is referenced by table state that must be preserved. Files that are not
   * reachable are candidates for deletion.
   */
  @SchemaFieldNumber("2")
  public abstract boolean getReachable();

  /** The identifier of the table the file belongs to. */
  @SchemaFieldNumber("3")
  public abstract String getTableIdentifier();

  /**
   * Creates a {@link FileInfo}.
   *
   * @throws NullPointerException if {@code path}, {@code category}, or {@code tableIdentifier} is
   *     null
   */
  public static FileInfo of(
      String path, FileCategory category, boolean reachable, String tableIdentifier) {
    return builder()
        .setPath(path)
        .setCategory(category)
        .setReachable(reachable)
        .setTableIdentifier(tableIdentifier)
        .build();
  }

  static Builder builder() {
    return new AutoValue_FileInfo.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setPath(String path);

    abstract Builder setCategory(FileCategory category);

    abstract Builder setReachable(boolean reachable);

    abstract Builder setTableIdentifier(String tableIdentifier);

    abstract FileInfo build();
  }
}
