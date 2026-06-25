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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.checkerframework.checker.nullness.qual.Nullable;

class FileScanTaskSorter {

  /** Simple sorting by table's sort order using min/max values from file statistics */
  static List<FileScanTask> sortByTableSortOrder(
      CloseableIterable<FileScanTask> tasks, SortOrder sortOrder) {

    List<FileScanTask> taskList = Lists.newArrayList(tasks);
    if (sortOrder.fields().isEmpty()) {
      return taskList;
    }

    taskList.sort(new SortOrderComparator(sortOrder));

    return taskList;
  }

  /** Simple sorting by a single column's min value */
  static List<FileScanTask> sortByColumn(CloseableIterable<FileScanTask> tasks, SortField field)
      throws IOException {

    try {
      List<FileScanTask> taskList = Lists.newArrayList(tasks);
      taskList.sort(
          (task1, task2) -> {
            Object min1 = getMinValue(task1, field.sourceId());
            Object min2 = getMinValue(task2, field.sourceId());
            return compareNullableValues(min1, min2, field.nullOrder());
          });
      return taskList;
    } finally {
      tasks.close();
    }
  }

  private static class SortOrderComparator implements Comparator<FileScanTask> {
    private final List<SortField> sortFields;

    SortOrderComparator(SortOrder sortOrder) {
      this.sortFields = sortOrder.fields();
    }

    @Override
    public int compare(FileScanTask task1, FileScanTask task2) {
      for (SortField sortField : sortFields) {
        int fieldId = sortField.sourceId();

        int comparison;
        if (sortField.direction() == SortDirection.ASC) {
          comparison =
              compareNullableValues(
                  getMinValue(task1, fieldId), getMinValue(task2, fieldId), sortField.nullOrder());

        } else {
          comparison =
              compareNullableValues(
                  getMaxValue(task2, fieldId), getMaxValue(task1, fieldId), sortField.nullOrder());
        }

        if (comparison != 0) {
          return comparison;
        }
      }

      // if all sort columns are equal, choose the task that starts with an earlier offset
      return Long.compare(task1.start(), task2.start());
    }
  }

  @SuppressWarnings("unchecked")
  private static int compareNullableValues(
      @Nullable Object min1, @Nullable Object min2, NullOrder nullOrder) {
    if (min1 == null && min2 == null) {
      return 0;
    }
    if (nullOrder == NULLS_FIRST) {
      if (min1 == null) {
        return -1;
      }
      if (min2 == null) {
        return 1;
      }
    } else if (nullOrder == NULLS_LAST) {
      if (min1 == null) {
        return 1;
      }
      if (min2 == null) {
        return -1;
      }
    }

    if (min1 instanceof Comparable && min2 instanceof Comparable) {
      Comparable<Object> comparable1 = (Comparable<Object>) min1;
      return comparable1.compareTo(min2);
    }
    return 0;
  }

  private static @Nullable Object getMinValue(FileScanTask task, int columnId) {
    @Nullable ByteBuffer value = task.file().lowerBounds().get(columnId);
    if (value == null) {
      return null;
    }
    return Conversions.fromByteBuffer(task.schema().findType(columnId), value);
  }

  private static @Nullable Object getMaxValue(FileScanTask task, int columnId) {
    @Nullable ByteBuffer value = task.file().upperBounds().get(columnId);
    if (value == null) {
      return null;
    }
    return Conversions.fromByteBuffer(task.schema().findType(columnId), value);
  }
}
