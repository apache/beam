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
package org.apache.beam.sdk.io.delta;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A serializable task containing the necessary metadata to read a group of files in a Delta table.
 * Packs both the {@code scanFileRows} (representing the physical files and deletion vectors) and
 * {@code scanStateRow} (containing snapshot-level read schemas, configuration, and options).
 */
public class DeltaReadTask implements Serializable {
  private static final long serialVersionUID = 1L;

  private final List<SerializableRow> scanFileRows;
  private final SerializableRow scanStateRow;

  public DeltaReadTask(List<SerializableRow> scanFileRows, SerializableRow scanStateRow) {
    this.scanFileRows = scanFileRows;
    this.scanStateRow = scanStateRow;
  }

  public List<SerializableRow> getScanFileRows() {
    return scanFileRows;
  }

  public SerializableRow getScanStateRow() {
    return scanStateRow;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeltaReadTask)) {
      return false;
    }
    DeltaReadTask that = (DeltaReadTask) o;
    return Objects.equals(scanFileRows, that.scanFileRows)
        && Objects.equals(scanStateRow, that.scanStateRow);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scanFileRows, scanStateRow);
  }

  @Override
  public String toString() {
    return "DeltaReadTask{"
        + "scanFileRows="
        + scanFileRows
        + ", scanStateRow="
        + scanStateRow
        + '}';
  }
}
