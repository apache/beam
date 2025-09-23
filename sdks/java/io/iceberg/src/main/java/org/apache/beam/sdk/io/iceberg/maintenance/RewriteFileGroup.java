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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.ScanTaskParser;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class RewriteFileGroup {
  static Builder builder() {
    return new AutoValue_RewriteFileGroup.Builder();
  }

  int numInputFiles() {
    return getJsonTasks().size();
  }

  abstract int getGlobalIndex();

  abstract int getPartitionIndex();

  abstract String getPartitionPath();

  abstract List<String> getJsonTasks();

  abstract int getOutputSpecId();

  abstract long getWriteMaxFileSize();

  abstract long getInputSplitSize();

  abstract int getExpectedOutputFiles();

  abstract long getTotalInputFileByteSize();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setGlobalIndex(int globalIndex);

    abstract Builder setPartitionIndex(int partitionIndex);

    abstract Builder setPartitionPath(String partitionPath);

    abstract Builder setJsonTasks(List<String> jsonTasks);

    Builder setFileScanTasks(List<FileScanTask> tasks) {
      long byteSize = tasks.stream().mapToLong(FileScanTask::length).sum();
      return setTotalInputFileByteSize(byteSize)
          .setJsonTasks(tasks.stream().map(ScanTaskParser::toJson).collect(Collectors.toList()));
    }

    abstract Builder setOutputSpecId(int outputSpecId);

    abstract Builder setWriteMaxFileSize(long writeMaxFileSize);

    abstract Builder setInputSplitSize(long inputSplitSize);

    abstract Builder setExpectedOutputFiles(int expectedOutputFiles);

    abstract Builder setTotalInputFileByteSize(long byteSize);

    abstract RewriteFileGroup build();
  }

  public static Comparator<RewriteFileGroup> comparator(RewriteJobOrder rewriteJobOrder) {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(RewriteFileGroup::getTotalInputFileByteSize);
      case BYTES_DESC:
        return Comparator.comparing(
            RewriteFileGroup::getTotalInputFileByteSize, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewriteFileGroup::numInputFiles);
      case FILES_DESC:
        return Comparator.comparing(RewriteFileGroup::numInputFiles, Comparator.reverseOrder());
      default:
        return (unused, unused2) -> 0;
    }
  }
}
