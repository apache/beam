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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Base class for rel test. */
public abstract class BaseRelTest {
  private static Map<String, BeamSqlTable> tables = new HashMap<>();
  protected static BeamSqlEnv env = BeamSqlEnv.readOnly("test", tables);

  protected static PCollection<Row> compilePipeline(String sql, Pipeline pipeline) {
    return BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery(sql));
  }

  protected static void registerTable(String tableName, BeamSqlTable table) {
    tables.put(tableName, table);
  }

  protected static BeamSqlTable getTable(String tableName) {
    return tables.get(tableName);
  }
}
