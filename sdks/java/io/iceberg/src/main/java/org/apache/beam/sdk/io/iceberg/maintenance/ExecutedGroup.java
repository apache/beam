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
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;

/** Represents the result of one rewritten subgroup as <b>compact commit descriptors</b>. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ExecutedGroup {

  @SchemaFieldNumber("0")
  public abstract long getStartingSnapshotId();

  /** The rewrite operation's id used to stamp the commit. */
  @SchemaFieldNumber("1")
  public abstract String getOperationId();

  /** Index of the planned parent group this subgroup belongs to. */
  @SchemaFieldNumber("2")
  public abstract int getParentGroupIndex();

  /** Total number of subgroups the parent was split into. */
  @SchemaFieldNumber("3")
  public abstract int getParentSubgroupCount();

  /** Total input byte size of this group, for partial-progress failure accounting. */
  @SchemaFieldNumber("4")
  public abstract long getTotalInputByteSize();

  /** Newly written compacted data files to ADD (full metrics). */
  @SchemaFieldNumber("5")
  public abstract List<SerializableDataFile> getNewFiles();

  /** Rewritten input data files to DELETE (no metrics). */
  @SchemaFieldNumber("6")
  public abstract List<SerializableDataFile> getRewrittenDataFiles();

  /** Dangling deletion vector JSONs to DELETE. */
  @SchemaFieldNumber("7")
  public abstract List<String> getDanglingDeleteFileJsons();

  /** The starting snapshot's sequence number; the floor for the commit's idempotency stamp scan. */
  @SchemaFieldNumber("8")
  public abstract long getStartingSequenceNumber();

  public static Builder builder() {
    return new AutoValue_ExecutedGroup.Builder();
  }

  /**
   * Locations of every newly written output file across {@code groups}.
   *
   * <p>After a failed/aborted commit, these become orphans (not committed anywhere). We tag them
   * with operation-id to make them discoverable for a remove-orphan-files run.
   */
  static List<String> newFilePaths(Iterable<ExecutedGroup> groups) {
    List<String> paths = new ArrayList<>();
    for (ExecutedGroup g : groups) {
      for (SerializableDataFile sdf : g.getNewFiles()) {
        paths.add(sdf.getPath());
      }
    }
    return paths;
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setStartingSnapshotId(long v);

    public abstract Builder setStartingSequenceNumber(long v);

    public abstract Builder setOperationId(String v);

    public abstract Builder setParentGroupIndex(int v);

    public abstract Builder setParentSubgroupCount(int v);

    public abstract Builder setTotalInputByteSize(long v);

    public abstract Builder setNewFiles(List<SerializableDataFile> v);

    public abstract Builder setRewrittenDataFiles(List<SerializableDataFile> v);

    public abstract Builder setDanglingDeleteFileJsons(List<String> v);

    public abstract ExecutedGroup build();
  }
}
