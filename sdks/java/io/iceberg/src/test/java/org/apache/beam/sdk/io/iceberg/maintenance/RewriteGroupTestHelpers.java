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
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Test helpers that reconstruct a {@link RewriteSubGroup}'s planned range tasks from its compact
 * {@link TaskDescriptor}s. Reconstruction needs the table's partition specs, so these moved out of
 * {@code RewriteFileGroup} itself when C1 replaced the self-contained per-task JSON payload with
 * compact descriptors. Only tests need whole reconstructed tasks; production reads via {@code
 * TaskDescriptor.toScanTask} directly in {@code RewriteGroupDoFn}.
 */
final class RewriteGroupTestHelpers {
  private RewriteGroupTestHelpers() {}

  /** The group's range tasks, reconstructed against {@code table.specs()}. */
  static List<FileScanTask> tasks(RewriteSubGroup group, Table table) {
    return group.getTaskDescriptors().stream()
        .map(d -> d.toScanTask(table.specs()))
        .collect(Collectors.toList());
  }

  /** The old data files this group replaces — exactly the rewritten inputs. */
  static List<DataFile> rewrittenDataFiles(RewriteSubGroup group, Table table) {
    return tasks(group, table).stream().map(FileScanTask::file).collect(Collectors.toList());
  }

  /** File-scoped deletion vectors among the inputs, removed in the same rewrite commit. */
  static List<DeleteFile> danglingDeleteFiles(RewriteSubGroup group, Table table) {
    return tasks(group, table).stream()
        .flatMap(t -> t.deletes().stream().filter(ContentFileUtil::isDV))
        .collect(Collectors.toList());
  }
}
