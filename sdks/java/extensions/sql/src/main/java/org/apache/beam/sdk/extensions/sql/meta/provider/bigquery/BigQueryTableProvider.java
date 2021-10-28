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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.MoreObjects.firstNonNull;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;

/**
 * BigQuery table provider.
 *
 * <p>A sample of text table is:
 *
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   ID INT COMMENT 'this is the primary key',
 *   NAME VARCHAR(127) COMMENT 'this is the name'
 * )
 * TYPE 'bigquery'
 * COMMENT 'this is the table orders'
 * LOCATION '[PROJECT_ID]:[DATASET].[TABLE]'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class BigQueryTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "bigquery";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new BigQueryTable(table, getConversionOptions(table.getProperties()));
  }

  protected static ConversionOptions getConversionOptions(JSONObject properties) {
    return ConversionOptions.builder()
        .setTruncateTimestamps(
            firstNonNull(properties.getBoolean("truncateTimestamps"), false)
                ? TruncateTimestamps.TRUNCATE
                : TruncateTimestamps.REJECT)
        .build();
  }
}
