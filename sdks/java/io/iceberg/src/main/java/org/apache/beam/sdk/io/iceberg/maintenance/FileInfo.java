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
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** Represents a file entry evaluated during snapshot expiration. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class FileInfo implements Serializable {

  /** Fully qualified URI or storage path of the file. */
  @SchemaFieldNumber("0")
  public abstract String getPath();

  /** Category of the file (data, delete, manifest, manifest list, statistics). */
  @SchemaFieldNumber("1")
  public abstract String getCategory();

  /** Whether this file is reachable and valid in a retained snapshot. */
  @SchemaFieldNumber("2")
  public abstract boolean getValid();

  /** Associated Iceberg table identifier string. */
  @SchemaFieldNumber("3")
  public abstract String getTableIdentifier();

  public static Builder builder() {
    return new AutoValue_FileInfo.Builder();
  }

  public static FileInfo of(
      String path, FileCategory category, boolean valid, String tableIdentifier) {
    Preconditions.checkNotNull(tableIdentifier, "tableIdentifier must not be null");
    return builder()
        .setPath(path)
        .setCategory(category.name())
        .setValid(valid)
        .setTableIdentifier(tableIdentifier)
        .build();
  }

  public FileCategory fileCategory() {
    return FileCategory.valueOf(getCategory());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPath(String path);

    public abstract Builder setCategory(String category);

    public abstract Builder setValid(boolean valid);

    public abstract Builder setTableIdentifier(String tableIdentifier);

    public abstract FileInfo build();
  }
}
