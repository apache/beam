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
package org.apache.beam.sdk.io.snowflake.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

/**
 * POJO representing schema of Table in Snowflake. Used by {@link SnowflakeIO.Write} when {
 *
 * @link org.apache.beam.sdk.io.snowflake.SnowflakeIO.Write.CreateDisposition#CREATE_IF_NEEDED}
 *     disposition is used.
 */
public class SnowflakeTableSchema implements Serializable {

  @JsonProperty("schema")
  private SnowflakeColumn[] columns;

  public static SnowflakeTableSchema of(SnowflakeColumn... columns) {
    return new SnowflakeTableSchema(columns);
  }

  public SnowflakeTableSchema() {}

  public SnowflakeTableSchema(SnowflakeColumn... columns) {
    this.columns = columns;
  }

  public String sql() {
    List<String> columnsSqls = new ArrayList<>();
    for (SnowflakeColumn column : columns) {
      columnsSqls.add(column.sql());
    }

    return Joiner.on(", ").join(columnsSqls);
  }

  public SnowflakeColumn[] getColumns() {
    return columns;
  }

  public void setColumns(SnowflakeColumn[] columns) {
    this.columns = columns;
  }
}
