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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * {@link TableProvider} for {@link BigtableTable}.
 *
 * <p>A sample of BigTable table is:
 *
 * <pre>{@code
 *   CREATE EXTERNAL TABLE beamTable(
 *     key VARCHAR NOT NULL,
 *     familyTest ROW<
 *       boolColumn BOOLEAN NOT NULL,
 *       longColumnWithTsAndLabels ROW<
 *         val BIGINT NOT NULL,
 *         timestampMicros BIGINT NOT NULL,
 *         labels VARCHAR
 *       > NOT NULL,
 *       stringArrayColumn ARRAY<VARCHAR> NOT NULL,
 *       doubleColumn DOUBLE NOT NULL,
 *       binaryColumn BINARY NOT NULL
 *     > NOT NULL
 *   )
 *   TYPE 'bigtable'
 *   LOCATION 'googleapis.com/bigtable/projects/<project_id>/instances/<instance_id>/tables/<table_name>'
 * </pre>
 *
 * <p>Flat-schema table is also supported. It requires an additional property "columnsMapping" which
 * describes which familyColumn describes which value in the form "familyName:columnQualifier":
 *
 * <pre>{@code
 *   CREATE EXTERNAL TABLE beamTable(
 *     key VARCHAR NOT NULL,
 *     boolColumn BOOLEAN NOT NULL,
 *     longColumn BIGINT NOT NULL,
 *     stringColumn VARCHAR NOT NULL,
 *     doubleColumn DOUBLE NOT NULL,
 *     binaryColumn BINARY NOT NULL
 *   )
 *   TYPE 'bigtable'
 *   LOCATION 'googleapis.com/bigtable/projects/<project_id>/instances/<instance_id>/tables/<table_name>'
 *   TBLPROPERTIES '{
 *     "columnsMapping": "f:boolColumn,f:longColumn,f:stringColumn,f2:doubleColumn,f2:binaryColumn"
 *   }'
 * </pre>
 */
@Internal
@AutoService(TableProvider.class)
public class BigtableTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "bigtable";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new BigtableTable(table);
  }
}
