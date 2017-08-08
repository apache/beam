/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.schema.transform;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.schema.BeamRecordSqlType;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;

/**
 * shared methods to test PTransforms which execute Beam SQL steps.
 *
 */
public class BeamTransformBaseTest {
  public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static BeamRecordSqlType inputRowType;
  public static List<BeamRecord> inputRows;

  @BeforeClass
  public static void prepareInput() throws NumberFormatException, ParseException{
    List<KV<String, SqlTypeName>> columnMetadata = Arrays.asList(
        KV.of("f_int", SqlTypeName.INTEGER), KV.of("f_long", SqlTypeName.BIGINT),
        KV.of("f_short", SqlTypeName.SMALLINT), KV.of("f_byte", SqlTypeName.TINYINT),
        KV.of("f_float", SqlTypeName.FLOAT), KV.of("f_double", SqlTypeName.DOUBLE),
        KV.of("f_string", SqlTypeName.VARCHAR), KV.of("f_timestamp", SqlTypeName.TIMESTAMP),
        KV.of("f_int2", SqlTypeName.INTEGER)
        );
    inputRowType = initTypeOfSqlRow(columnMetadata);
    inputRows = Arrays.asList(
        initBeamSqlRow(columnMetadata,
            Arrays.<Object>asList(1, 1000L, Short.valueOf("1"), Byte.valueOf("1"), 1.0F, 1.0,
                "string_row1", format.parse("2017-01-01 01:01:03"), 1)),
        initBeamSqlRow(columnMetadata,
            Arrays.<Object>asList(1, 2000L, Short.valueOf("2"), Byte.valueOf("2"), 2.0F, 2.0,
                "string_row2", format.parse("2017-01-01 01:02:03"), 2)),
        initBeamSqlRow(columnMetadata,
            Arrays.<Object>asList(1, 3000L, Short.valueOf("3"), Byte.valueOf("3"), 3.0F, 3.0,
                "string_row3", format.parse("2017-01-01 01:03:03"), 3)),
        initBeamSqlRow(columnMetadata, Arrays.<Object>asList(1, 4000L, Short.valueOf("4"),
            Byte.valueOf("4"), 4.0F, 4.0, "string_row4", format.parse("2017-01-01 02:04:03"), 4)));
  }

  /**
   * create a {@code BeamSqlRowType} for given column metadata.
   */
  public static BeamRecordSqlType initTypeOfSqlRow(List<KV<String, SqlTypeName>> columnMetadata){
    FieldInfoBuilder builder = BeamQueryPlanner.TYPE_FACTORY.builder();
    for (KV<String, SqlTypeName> cm : columnMetadata) {
      builder.add(cm.getKey(), cm.getValue());
    }
    return CalciteUtils.toBeamRowType(builder.build());
  }

  /**
   * Create an empty row with given column metadata.
   */
  public static BeamRecord initBeamSqlRow(List<KV<String, SqlTypeName>> columnMetadata) {
    return initBeamSqlRow(columnMetadata, Arrays.asList());
  }

  /**
   * Create a row with given column metadata, and values for each column.
   *
   */
  public static BeamRecord initBeamSqlRow(List<KV<String, SqlTypeName>> columnMetadata,
      List<Object> rowValues){
    BeamRecordSqlType rowType = initTypeOfSqlRow(columnMetadata);

    return new BeamRecord(rowType, rowValues);
  }

}
