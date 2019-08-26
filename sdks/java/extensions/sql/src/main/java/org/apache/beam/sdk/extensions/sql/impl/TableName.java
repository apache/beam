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
package org.apache.beam.sdk.extensions.sql.impl;

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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;

/** Represents a parsed table name that is specified in a FROM clause (and other places). */
@AutoValue
public abstract class TableName {

  /**
   * Table path up to the leaf table name.
   *
   * <p>Does not necessarily start from a schema name.
   *
   * <p>Does not include the actual table name, see {@link #getTableName()}.
   */
  public abstract List<String> getPath();

  /** Table name, the last element of the fully-specified table name with path. */
  public abstract String getTableName();

  /** Full table name with path. */
  public static TableName create(List<String> fullPath) {
    checkNotNull(fullPath, "Full table path cannot be null");
    checkArgument(fullPath.size() > 0, "Full table path has to have at least one element");
    return create(fullPath.subList(0, fullPath.size() - 1), fullPath.get(fullPath.size() - 1));
  }

  /** Table name plus the path up to but not including table name. */
  public static TableName create(List<String> path, String tableName) {
    checkNotNull(tableName, "Table name cannot be null");
    return new AutoValue_TableName(path == null ? Collections.emptyList() : path, tableName);
  }

  /** Whether it's a compound table name (with multiple path components). */
  public boolean isCompound() {
    return getPath().size() > 0;
  }

  /** Whether it's a simple name, with a single name component. */
  public boolean isSimple() {
    return getPath().size() == 0;
  }

  /** First element in the path. */
  public String getPrefix() {
    checkState(isCompound());
    return getPath().get(0);
  }

  /**
   * Remove prefix, e.g. this is helpful when stripping the top-level schema to register a table
   * name with a provider.
   */
  public TableName removePrefix() {
    List<String> pathPostfix = getPath().stream().skip(1).collect(toList());
    return TableName.create(pathPostfix, getTableName());
  }
}
