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
package org.apache.beam.it.cassandra.matchers;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.truthmatchers.RecordsSubject;

public class CassandraAsserts {

  /**
   * Convert Cassandra {@link Row} list to a list of maps.
   *
   * @param rows Rows to parse.
   * @return List of maps to use in {@link RecordsSubject}.
   */
  @SuppressWarnings("nullness")
  public static List<Map<String, Object>> cassandraRowsToRecords(Iterable<Row> rows) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Row row : rows) {
        Map<String, Object> converted = new HashMap<>();
        for (ColumnDefinition columnDefinition : row.getColumnDefinitions()) {

          Object value = null;
          if (columnDefinition.getType().equals(DataTypes.TEXT)) {
            value = row.getString(columnDefinition.getName());
          } else if (columnDefinition.getType().equals(DataTypes.INT)) {
            value = row.getInt(columnDefinition.getName());
          }
          converted.put(columnDefinition.getName().toString(), value);
        }
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Cassandra Rows to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param rows Records in Cassandra's {@link Row} format to use in the comparison.
   * @return Truth subject to chain assertions on.
   */
  public static RecordsSubject assertThatCassandraRecords(Iterable<Row> rows) {
    return assertThatRecords(cassandraRowsToRecords(rows));
  }
}
