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
import java.util.Map;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A serializable task containing the necessary metadata to read a CDF (Change Data Feed) file. This
 * can be either a CDC parquet file or a regular data file (representing inserts) from a commit.
 */
public class DeltaCDCReadTask implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String path;
  private final long size;
  private final Map<String, String> partitionValues;
  private final long version;
  private final long timestamp;
  private final boolean isCDC;
  private final List<Long> rowGroupSizes;
  private final SerializableRow scanStateRow;

  public DeltaCDCReadTask(
      String path,
      long size,
      Map<String, String> partitionValues,
      long version,
      long timestamp,
      boolean isCDC,
      List<Long> rowGroupSizes,
      SerializableRow scanStateRow) {
    this.path = path;
    this.size = size;
    this.partitionValues = partitionValues;
    this.version = version;
    this.timestamp = timestamp;
    this.isCDC = isCDC;
    this.rowGroupSizes = rowGroupSizes;
    this.scanStateRow = scanStateRow;
  }

  public String getPath() {
    return path;
  }

  public long getSize() {
    return size;
  }

  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  public long getVersion() {
    return version;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isCDC() {
    return isCDC;
  }

  public List<Long> getRowGroupSizes() {
    return rowGroupSizes;
  }

  public SerializableRow getScanStateRow() {
    return scanStateRow;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeltaCDCReadTask)) {
      return false;
    }
    DeltaCDCReadTask that = (DeltaCDCReadTask) o;
    return size == that.size
        && version == that.version
        && timestamp == that.timestamp
        && isCDC == that.isCDC
        && Objects.equals(path, that.path)
        && Objects.equals(partitionValues, that.partitionValues)
        && Objects.equals(rowGroupSizes, that.rowGroupSizes)
        && Objects.equals(scanStateRow, that.scanStateRow);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        path, size, partitionValues, version, timestamp, isCDC, rowGroupSizes, scanStateRow);
  }

  @Override
  public String toString() {
    return "DeltaCDCReadTask{"
        + "path='"
        + path
        + '\''
        + ", size="
        + size
        + ", partitionValues="
        + partitionValues
        + ", version="
        + version
        + ", timestamp="
        + timestamp
        + ", isCDC="
        + isCDC
        + ", rowGroupSizes="
        + rowGroupSizes
        + ", scanStateRow="
        + scanStateRow
        + '}';
  }
}
