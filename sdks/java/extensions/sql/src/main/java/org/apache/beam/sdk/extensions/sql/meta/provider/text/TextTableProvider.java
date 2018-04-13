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

import static org.apache.beam.sdk.extensions.sql.meta.provider.MetaUtils.getRowTypeFromTable;

import com.alibaba.fastjson.JSONObject;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;

/**
 * Text table provider.
 *
 * <p>A sample of text table is:
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   ID INT PRIMARY KEY COMMENT 'this is the primary key',
 *   NAME VARCHAR(127) COMMENT 'this is the name'
 * )
 * TYPE 'text'
 * COMMENT 'this is the table orders'
 * LOCATION '/home/admin/orders'
 * TBLPROPERTIES '{"format": "Excel"}' -- format of each text line(csv format)
 * }</pre>
 */
public class TextTableProvider implements TableProvider {

  @Override public String getTableType() {
    return "text";
  }

  @Override public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema schema = getRowTypeFromTable(table);

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

  @Override public void createTable(Table table) {
    // empty
  }

  @Override public void dropTable(String tableName) {
    // empty
  }

  @Override public List<Table> listTables() {
    return Collections.emptyList();
  }

  @Override public void init() {
    // empty
  }

  @Override public void close() {
    // empty
  }
}
