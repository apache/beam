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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;

/**
 * An interface that handles ALTER TABLE operations.
 *
 * <p>An instance is created and used when {@link TableProvider#alterTable(String)} is called.
 */
public interface AlterTableOps {
  /**
   * Updates a table's properties. Includes setting properties (which overwrites existing values),
   * and/or resetting properties (removes values of given keys).
   */
  void updateTableProperties(Map<String, String> setProps, List<String> resetProps);

  /** Updates a table's schema. Includes adding new columns and/or dropping existing columns. */
  void updateSchema(List<Schema.Field> columnsToAdd, Collection<String> columnsToDrop);

  /**
   * Updates a table's partition spec, if applicable. Includes adding new partitions and/or dropping
   * existing partitions.
   */
  void updatePartitionSpec(List<String> partitionsToAdd, Collection<String> partitionsToDrop);
}
