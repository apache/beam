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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class ReadTaskDescriptor implements Serializable {
  private transient @MonotonicNonNull CombinedScanTask cachedCombinedScanTask;
  private transient @MonotonicNonNull List<FileScanTask> cachedFileScanTasks;

  static Builder builder() {
    return new AutoValue_ReadTaskDescriptor.Builder();
  }

  abstract String getTableIdentifierString();

  abstract List<String> getFileScanTaskJsonList();

  abstract long getTotalByteSize();

  int numTasks() {
    return getFileScanTaskJsonList().size();
  }

  @SchemaIgnore
  CombinedScanTask getCombinedScanTask() {
    if (cachedCombinedScanTask == null) {
      cachedCombinedScanTask = new BaseCombinedScanTask(getFileScanTasks());
    }
    return cachedCombinedScanTask;
  }

  @SchemaIgnore
  private List<FileScanTask> getFileScanTasks() {
    if (cachedFileScanTasks == null) {
      cachedFileScanTasks =
          getFileScanTaskJsonList().stream()
              .map(json -> ScanTaskParser.fromJson(json, true))
              .collect(Collectors.toList());
    }
    return cachedFileScanTasks;
  }

  @SchemaIgnore
  FileScanTask getFileScanTask(long index) {
    return getFileScanTasks().get((int) index);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTableIdentifierString(String table);

    abstract Builder setFileScanTaskJsonList(List<String> fromSnapshot);

    abstract Builder setTotalByteSize(long byteSize);

    @SchemaIgnore
    Builder setCombinedScanTask(CombinedScanTask combinedScanTask) {
      List<FileScanTask> tasks = new ArrayList<>(combinedScanTask.tasks());
      List<String> jsonTasks =
          tasks.stream().map(ScanTaskParser::toJson).collect(Collectors.toList());
      return setFileScanTaskJsonList(jsonTasks);
    }

    abstract ReadTaskDescriptor build();
  }
}
