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

package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import com.alibaba.fastjson.JSONObject;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;

/**
 * Text table provider.
 *
 * <p>A sample of text table is:
 *
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   ID INT COMMENT 'this is the primary key',
 *   NAME VARCHAR(127) COMMENT 'this is the name'
 * )
 * TYPE 'text'
 * COMMENT 'this is the table orders'
 * LOCATION '/home/admin/orders'
 * TBLPROPERTIES '{"format": "Excel"}' -- format of each text line(csv format)
 * }</pre>
 */
public class TextTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "text";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema schema = table.getSchema();

    String filePattern = table.getLocation();
    CSVFormat format = CSVFormat.DEFAULT;
    JSONObject properties = table.getProperties();
    String csvFormatStr = properties.getString("format");
    if (csvFormatStr != null && !csvFormatStr.isEmpty()) {
      format = CSVFormat.valueOf(csvFormatStr);
    }

    BeamTextCSVTable txtTable = new BeamTextCSVTable(schema, filePattern, format);
    return txtTable;
  }
}
