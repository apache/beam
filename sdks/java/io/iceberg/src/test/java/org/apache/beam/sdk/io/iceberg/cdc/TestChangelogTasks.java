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
package org.apache.beam.sdk.io.iceberg.cdc;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;

/** Builds {@link SerializableChangelogTask}s for tests outside this package. */
public class TestChangelogTasks {
  private TestChangelogTasks() {}

  public static SerializableChangelogTask addedRows(
      Table table, DataFile dataFile, List<DeleteFile> addedDeletes) {
    return SerializableChangelogTask.builder()
        .setType(SerializableChangelogTask.Type.ADDED_ROWS)
        .setDataFile(dataFile, table.spec(), true)
        .setAddedDeletes(serializableDeletes(table, addedDeletes))
        .setExistingDeletes(ImmutableList.of())
        .setSpecId(table.spec().specId())
        .setOperation(ChangelogOperation.INSERT)
        .setOrdinal(0)
        .setCommitSnapshotId(1L)
        .setStart(0L)
        .setLength(dataFile.fileSizeInBytes())
        .setJsonExpression(ExpressionParser.toJson(Expressions.alwaysTrue()))
        .build();
  }

  private static List<SerializableDeleteFile> serializableDeletes(
      Table table, List<DeleteFile> deletes) {
    return deletes.stream()
        .map(
            delete ->
                SerializableDeleteFile.from(
                    delete, table.spec().partitionToPath(delete.partition()), true))
        .collect(Collectors.toList());
  }
}
