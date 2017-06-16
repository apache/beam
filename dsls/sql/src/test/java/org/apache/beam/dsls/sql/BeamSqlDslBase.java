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
package org.apache.beam.dsls.sql;

import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * prepare input records to test {@link BeamSql}.
 *
 * <p>Note that, any change in these records would impact tests in this package.
 *
 */
public class BeamSqlDslBase {
  public static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @ClassRule
  public static TestPipeline pipeline = TestPipeline.create();

  public static BeamSqlRecordType recordTypeInTableA;
  public static List<BeamSqlRow> recordsInTableA;

  public static PCollection<BeamSqlRow> inputA1;
  public static PCollection<BeamSqlRow> inputA2;

  @BeforeClass
  public static void prepareClass() throws ParseException {
    recordTypeInTableA = new BeamSqlRecordType();
    recordTypeInTableA.addField("f_int", Types.INTEGER);
    recordTypeInTableA.addField("f_long", Types.BIGINT);
    recordTypeInTableA.addField("f_short", Types.SMALLINT);
    recordTypeInTableA.addField("f_byte", Types.TINYINT);
    recordTypeInTableA.addField("f_float", Types.FLOAT);
    recordTypeInTableA.addField("f_double", Types.DOUBLE);
    recordTypeInTableA.addField("f_string", Types.VARCHAR);
    recordTypeInTableA.addField("f_timestamp", Types.TIMESTAMP);
    recordTypeInTableA.addField("f_int2", Types.INTEGER);

    recordsInTableA = prepareInputRecordsInTableA();

    inputA1 = PBegin.in(pipeline).apply("inputA1", Create.of(recordsInTableA)
        .withCoder(new BeamSqlRowCoder(recordTypeInTableA)));

    inputA2 = PBegin.in(pipeline).apply("inputA2", Create.of(recordsInTableA.get(0))
        .withCoder(new BeamSqlRowCoder(recordTypeInTableA)));
  }

  private static List<BeamSqlRow> prepareInputRecordsInTableA() throws ParseException{
    List<BeamSqlRow> rows = new ArrayList<>();

    BeamSqlRow row1 = new BeamSqlRow(recordTypeInTableA);
    row1.addField(0, 1);
    row1.addField(1, 1000L);
    row1.addField(2, Short.valueOf("1"));
    row1.addField(3, Byte.valueOf("1"));
    row1.addField(4, 1.0f);
    row1.addField(5, 1.0);
    row1.addField(6, "string_row1");
    row1.addField(7, FORMAT.parse("2017-01-01 01:01:03"));
    row1.addField(8, 0);
    rows.add(row1);

    BeamSqlRow row2 = new BeamSqlRow(recordTypeInTableA);
    row2.addField(0, 2);
    row2.addField(1, 2000L);
    row2.addField(2, Short.valueOf("2"));
    row2.addField(3, Byte.valueOf("2"));
    row2.addField(4, 2.0f);
    row2.addField(5, 2.0);
    row2.addField(6, "string_row2");
    row2.addField(7, FORMAT.parse("2017-01-01 01:02:03"));
    row2.addField(8, 0);
    rows.add(row2);

    BeamSqlRow row3 = new BeamSqlRow(recordTypeInTableA);
    row3.addField(0, 3);
    row3.addField(1, 3000L);
    row3.addField(2, Short.valueOf("3"));
    row3.addField(3, Byte.valueOf("3"));
    row3.addField(4, 3.0f);
    row3.addField(5, 3.0);
    row3.addField(6, "string_row3");
    row3.addField(7, FORMAT.parse("2017-01-01 01:06:03"));
    row3.addField(8, 0);
    rows.add(row3);

    BeamSqlRow row4 = new BeamSqlRow(recordTypeInTableA);
    row4.addField(0, 4);
    row4.addField(1, 4000L);
    row4.addField(2, Short.valueOf("4"));
    row4.addField(3, Byte.valueOf("4"));
    row4.addField(4, 4.0f);
    row4.addField(5, 4.0);
    row4.addField(6, "string_row4");
    row4.addField(7, FORMAT.parse("2017-01-01 02:04:03"));
    row4.addField(8, 0);
    rows.add(row4);

    return rows;
  }


}
