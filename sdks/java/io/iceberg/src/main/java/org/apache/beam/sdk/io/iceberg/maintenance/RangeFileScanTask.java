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

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * A worker-side {@link FileScanTask} reconstructed from a {@link TaskDescriptor}. Represents one
 * row-group range of a data file, plus the delete files that apply to it.
 */
class RangeFileScanTask implements FileScanTask {
  private final DataFile file;
  private final List<DeleteFile> deletes;
  private final long start;
  private final long length;
  private final PartitionSpec spec;

  RangeFileScanTask(
      DataFile file, List<DeleteFile> deletes, long start, long length, PartitionSpec spec) {
    this.file = file;
    this.deletes = deletes;
    this.start = start;
    this.length = length;
    this.spec = spec;
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public List<DeleteFile> deletes() {
    return deletes;
  }

  @Override
  public long start() {
    return start;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    throw new UnsupportedOperationException(
        "RangeFileScanTask is already a fixed row-group range and cannot be re-split.");
  }
}
