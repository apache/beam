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

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/** A {@code InMemoryMetaTableProvider} is an abstract {@code TableProvider} for in-memory types. */
@Internal
public abstract class InMemoryMetaTableProvider implements TableProvider {

  @Override
  public void createTable(Table table) {
    // No-op
  }

  @Override
  public void dropTable(String tableName) {
    // No-op
  }

  @Override
  public Map<String, Table> getTables() {
    return Collections.emptyMap();
  }
}
