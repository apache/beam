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
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class RewriteSubGroup {
  static Builder builder() {
    return new AutoValue_RewriteSubGroup.Builder();
  }

  @SchemaFieldNumber("0")
  abstract int getGlobalIndex();

  /** Index of the planned parent group this subgroup belongs to; shared by all its subgroups. */
  @SchemaFieldNumber("1")
  abstract int getParentGroupIndex();

  /** Total number of subgroups belonging to the parent. */
  @SchemaFieldNumber("2")
  abstract int getParentSubgroupCount();

  /** The compact per-range descriptors this subgroup rewrites (one per row-group range). */
  @SchemaFieldNumber("3")
  abstract List<TaskDescriptor> getTaskDescriptors();

  @SchemaFieldNumber("4")
  abstract int getOutputSpecId();

  @SchemaFieldNumber("5")
  abstract long getWriteMaxFileSize();

  @SchemaFieldNumber("6")
  abstract long getTotalInputFileByteSize();

  @SchemaFieldNumber("7")
  abstract long getStartingSnapshotId();

  /**
   * The rewrite operation's id, unique to this pipeline execution. Used to name/tag output files
   * and to stamp commits for idempotency.
   */
  @SchemaFieldNumber("8")
  abstract String getOperationId();

  /**
   * The starting snapshot's own sequence number, captured at planning. It floors the commit's
   * idempotency stamp scan, and still bounds the walk if that snapshot has since been expired.
   */
  @SchemaFieldNumber("9")
  abstract long getStartingSequenceNumber();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setGlobalIndex(int globalIndex);

    abstract Builder setParentGroupIndex(int parentGroupIndex);

    abstract Builder setParentSubgroupCount(int parentSubgroupCount);

    abstract Builder setTaskDescriptors(List<TaskDescriptor> taskDescriptors);

    /**
     * Builds compact per-range descriptors from planned range tasks and records the group's total
     * input byte size (the summed range lengths).
     */
    Builder setFileScanTasks(List<FileScanTask> tasks, Map<Integer, PartitionSpec> specs) {
      long byteSize = 0;
      List<TaskDescriptor> taskDescriptors = new ArrayList<>(tasks.size());
      for (FileScanTask task : tasks) {
        byteSize += task.length();
        taskDescriptors.add(TaskDescriptor.from(task, specs));
      }
      return setTotalInputFileByteSize(byteSize).setTaskDescriptors(taskDescriptors);
    }

    abstract Builder setOutputSpecId(int outputSpecId);

    abstract Builder setWriteMaxFileSize(long writeMaxFileSize);

    abstract Builder setTotalInputFileByteSize(long byteSize);

    abstract Builder setStartingSnapshotId(long startingSnapshotId);

    abstract Builder setStartingSequenceNumber(long startingSequenceNumber);

    abstract Builder setOperationId(String operationId);

    abstract RewriteSubGroup build();
  }
}
