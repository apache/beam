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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.iceberg.FileContent;

/**
 * Classification of the files handled by Iceberg table maintenance operations.
 *
 * <p>Values are encoded by their declaration index, so new values must only be appended and
 * existing values must not be reordered or removed.
 */
@Internal
public enum FileCategory {
  /** A data file containing table rows. */
  DATA,

  /** A row-level position delete file, including deletion vectors. */
  POSITION_DELETES,

  /** A row-level equality delete file. */
  EQUALITY_DELETES,

  /** A manifest file, which references data, delete, or (in format v4) other manifest files. */
  MANIFEST,

  /** A manifest list, which references the manifests of a single snapshot. */
  MANIFEST_LIST,

  /** A statistics or partition statistics file. */
  STATISTICS,

  /** Other table metadata, such as {@code metadata.json} files and {@code version-hint.text}. */
  OTHER_METADATA,

  /** A file found in storage whose role could not be determined. */
  UNKNOWN;

  /**
   * Returns the category for a manifest entry with the given content type.
   *
   * <p>Format v4 manifests can reference other manifests; those entries ({@link
   * FileContent#DATA_MANIFEST} and {@link FileContent#DELETE_MANIFEST}) map to {@link #MANIFEST}.
   */
  public static FileCategory fromContent(FileContent content) {
    switch (content) {
      case DATA:
        return DATA;
      case POSITION_DELETES:
        return POSITION_DELETES;
      case EQUALITY_DELETES:
        return EQUALITY_DELETES;
      case DATA_MANIFEST:
      case DELETE_MANIFEST:
        return MANIFEST;
    }
    throw new IllegalArgumentException("Unsupported file content: " + content);
  }
}
