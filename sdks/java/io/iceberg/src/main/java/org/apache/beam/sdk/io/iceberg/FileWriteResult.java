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
import java.io.IOException;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
abstract class FileWriteResult {

  private transient @MonotonicNonNull TableIdentifier cachedTableIdentifier;
  private transient @MonotonicNonNull ManifestFile cachedManifestFile;

  abstract String getTableIdentifierString();

  @SuppressWarnings("mutable")
  abstract byte[] getManifestFileBytes();

  @SchemaIgnore
  public TableIdentifier getTableIdentifier() {
    if (cachedTableIdentifier == null) {
      cachedTableIdentifier = TableIdentifier.parse(getTableIdentifierString());
    }
    return cachedTableIdentifier;
  }

  @SchemaIgnore
  public ManifestFile getManifestFile() {
    if (cachedManifestFile == null) {
      try {
        cachedManifestFile = ManifestFiles.decode(getManifestFileBytes());
      } catch (IOException exc) {
        throw new RuntimeException("Error decoding manifest file bytes");
      }
    }
    return cachedManifestFile;
  }

  public static Builder builder() {
    return new AutoValue_FileWriteResult.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTableIdentifierString(String tableIdString);

    abstract Builder setManifestFileBytes(byte[] manifestFileBytes);

    @SchemaIgnore
    public Builder setTableIdentifier(TableIdentifier tableId) {
      return setTableIdentifierString(tableId.toString());
    }

    @SchemaIgnore
    public Builder setManifestFile(ManifestFile manifestFile) throws IOException {
      return setManifestFileBytes(ManifestFiles.encode(manifestFile));
    }

    public abstract FileWriteResult build();
  }
}
