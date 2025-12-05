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
package org.apache.beam.sdk.extensions.sql.meta.provider.datagen;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * The service entry point for the 'datagen' table type.
 *
 * <p>This provider allows for the creation of SQL-configurable test data sources. Tables of this
 * type are defined using the {@code CREATE EXTERNAL TABLE} statement.
 *
 * <p>The provider supports generating both bounded data (for batch pipelines) using the {@code
 * "number-of-rows"} property, and unbounded data (for streaming pipelines) using the {@code
 * "rows-per-second"} property.
 *
 * <pre>{@code
 * CREATE EXTERNAL TABLE user_clicks (
 * event_id BIGINT,
 * user_id VARCHAR,
 * click_timestamp TIMESTAMP,
 * score DOUBLE
 * )
 * TYPE 'datagen'
 * TBLPROPERTIES '{
 * "rows-per-second": "100",
 *
 * "fields.event_id.kind": "sequence",
 * "fields.event_id.start": "1",
 * "fields.event_id.end": "1000000",
 *
 * "fields.user_id.kind": "random",
 * "fields.user_id.length": "12",
 *
 * "fields.click_timestamp.kind": "random",
 * "fields.click_timestamp.max-past": "60000",
 *
 * "fields.score.kind": "random",
 * "fields.score.min": "0.0",
 * "fields.score.max": "1.0",
 * "fields.score.null-rate": "0.1"
 * }'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class DataGeneratorTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "datagen";
  }

  /**
   * Instantiates the {@link DataGeneratorTable} when a {@code CREATE EXTERNAL TABLE} statement with
   * {@code TYPE 'datagen'} is executed.
   */
  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new DataGeneratorTable(table.getSchema(), table.getProperties());
  }
}
