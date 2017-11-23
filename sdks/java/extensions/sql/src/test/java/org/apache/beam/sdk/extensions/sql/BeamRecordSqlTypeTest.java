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

package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Types;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link BeamRecordSqlType}.
 */
public class BeamRecordSqlTypeTest {

  private static final List<Integer> TYPES = ImmutableList.of(
      Types.TINYINT,
      Types.SMALLINT,
      Types.INTEGER,
      Types.BIGINT,
      Types.FLOAT,
      Types.DOUBLE,
      Types.DECIMAL,
      Types.BOOLEAN,
      Types.CHAR,
      Types.VARCHAR,
      Types.TIME,
      Types.DATE,
      Types.TIMESTAMP);

  private static final List<String> NAMES = ImmutableList.of(
      "TINYINT_FIELD",
      "SMALLINT_FIELD",
      "INTEGER_FIELD",
      "BIGINT_FIELD",
      "FLOAT_FIELD",
      "DOUBLE_FIELD",
      "DECIMAL_FIELD",
      "BOOLEAN_FIELD",
      "CHAR_FIELD",
      "VARCHAR_FIELD",
      "TIME_FIELD",
      "DATE_FIELD",
      "TIMESTAMP_FIELD");

  private static final List<String> MORE_NAMES = ImmutableList.of(
      "ANOTHER_TINYINT_FIELD",
      "ANOTHER_SMALLINT_FIELD",
      "ANOTHER_INTEGER_FIELD",
      "ANOTHER_BIGINT_FIELD",
      "ANOTHER_FLOAT_FIELD",
      "ANOTHER_DOUBLE_FIELD",
      "ANOTHER_DECIMAL_FIELD",
      "ANOTHER_BOOLEAN_FIELD",
      "ANOTHER_CHAR_FIELD",
      "ANOTHER_VARCHAR_FIELD",
      "ANOTHER_TIME_FIELD",
      "ANOTHER_DATE_FIELD",
      "ANOTHER_TIMESTAMP_FIELD");

  @Test
  public void testBuildsWithCorrectFields() throws Exception {
    BeamRecordSqlType.Builder recordTypeBuilder = BeamRecordSqlType.builder();

    for (int i = 0; i < TYPES.size(); i++) {
      recordTypeBuilder.withField(NAMES.get(i), TYPES.get(i));
    }

    recordTypeBuilder.withTinyIntField(MORE_NAMES.get(0));
    recordTypeBuilder.withSmallIntField(MORE_NAMES.get(1));
    recordTypeBuilder.withIntegerField(MORE_NAMES.get(2));
    recordTypeBuilder.withBigIntField(MORE_NAMES.get(3));
    recordTypeBuilder.withFloatField(MORE_NAMES.get(4));
    recordTypeBuilder.withDoubleField(MORE_NAMES.get(5));
    recordTypeBuilder.withDecimalField(MORE_NAMES.get(6));
    recordTypeBuilder.withBooleanField(MORE_NAMES.get(7));
    recordTypeBuilder.withCharField(MORE_NAMES.get(8));
    recordTypeBuilder.withVarcharField(MORE_NAMES.get(9));
    recordTypeBuilder.withTimeField(MORE_NAMES.get(10));
    recordTypeBuilder.withDateField(MORE_NAMES.get(11));
    recordTypeBuilder.withTimestampField(MORE_NAMES.get(12));

    BeamRecordSqlType recordSqlType = recordTypeBuilder.build();

    List<String> expectedNames = ImmutableList.<String>builder()
        .addAll(NAMES)
        .addAll(MORE_NAMES)
        .build();

    List<Integer> expectedTypes = ImmutableList.<Integer>builder()
        .addAll(TYPES)
        .addAll(TYPES)
        .build();

    assertEquals(expectedNames, recordSqlType.getFieldNames());
    assertEquals(expectedTypes, recordSqlType.getFieldTypes());
  }
}
