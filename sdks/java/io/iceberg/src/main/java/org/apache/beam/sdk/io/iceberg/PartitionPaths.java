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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Partition values in a file's path. With prefix {@code gs://lake/orders/}, the file {@code
 * gs://lake/orders/dt=2024-01-15/f.parquet} has {@code dt = 2024-01-15}. If the table is
 * partitioned by {@code dt} as is (not by a transform such as {@code bucket}), readers take {@code
 * dt} from that value, so the file needs no {@code dt} column.
 */
final class PartitionPaths {
  /**
   * Hive writes {@code dt=__HIVE_DEFAULT_PARTITION__} when dt is null; Iceberg reads it as null.
   */
  static final String HIVE_NULL = "__HIVE_DEFAULT_PARTITION__";

  private PartitionPaths() {}

  /**
   * With prefix {@code gs://lake/orders/}: {@code gs://lake/orders/dt=1/region=EU/f.parquet} gives
   * {@code dt=1/region=EU}, and {@code gs://lake/orders/f.parquet} gives "".
   */
  static String relative(String filePath, String prefix) {
    String partitionPath = filePath.substring(prefix.length());
    int lastSlashIndex = partitionPath.lastIndexOf('/');
    return lastSlashIndex > 0 ? partitionPath.substring(0, lastSlashIndex) : "";
  }

  /**
   * The partition keys the file's folders give a value: {@code dt=1/region=EU/f.parquet} gives
   * {@code [dt, region]}, {@code dt=__HIVE_DEFAULT_PARTITION__/region=EU/f.parquet} gives {@code
   * [region]}, and {@code 2024/f.parquet} gives none. Also none without a prefix or for a file
   * outside it. Needs no table, unlike Iceberg's parser, so the pre-pass can use it.
   */
  static Set<String> partitionKeysInPath(String filePath, @Nullable String prefix) {
    Set<String> keys = new TreeSet<>();
    if (prefix == null || prefix.isEmpty() || !filePath.startsWith(prefix)) {
      return keys;
    }
    String relative = relative(filePath, prefix);
    if (relative.isEmpty()) {
      return keys;
    }
    for (String folder : relative.split("/", -1)) {
      String[] parts = folder.split("=", 2);
      if (parts.length == 2 && !parts[0].isEmpty() && !HIVE_NULL.equals(parts[1])) {
        keys.add(parts[0]);
      }
    }
    return keys;
  }

  /**
   * Ids of the table columns these keys fill. Only plain partitions count: a file under {@code
   * id_bucket=3/} in a table partitioned by {@code bucket(id, 4)} is in bucket 3, which does not
   * say what its {@code id} is.
   */
  static Set<Integer> columnsFilledByKeys(PartitionSpec spec, Collection<String> keys) {
    Set<Integer> ids = new HashSet<>();
    for (PartitionField field : spec.fields()) {
      if (field.transform().isIdentity() && keys.contains(field.name())) {
        ids.add(field.sourceId());
      }
    }
    return ids;
  }
}
