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
package org.apache.beam.sdk.io.gcp.testing;

import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.LABELS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import java.time.Instant;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigtableTestUtils {

  public static final String KEY1 = "key1";
  public static final String KEY2 = "key2";

  public static final String BOOL_COLUMN = "boolColumn";
  public static final String LONG_COLUMN = "longColumn";
  public static final String STRING_COLUMN = "stringColumn";
  public static final String DOUBLE_COLUMN = "doubleColumn";
  public static final String BINARY_COLUMN = "binaryColumn";
  public static final String FAMILY_TEST = "familyTest";

  public static final Schema LONG_COLUMN_SCHEMA =
      Schema.builder()
          .addInt64Field(VALUE)
          .addInt64Field(TIMESTAMP_MICROS)
          .addArrayField(LABELS, Schema.FieldType.STRING)
          .build();

  public static final Schema TEST_FAMILY_SCHEMA =
      Schema.builder()
          .addBooleanField(BOOL_COLUMN)
          .addRowField(LONG_COLUMN, LONG_COLUMN_SCHEMA)
          .addArrayField(STRING_COLUMN, Schema.FieldType.STRING)
          .addDoubleField(DOUBLE_COLUMN)
          .addByteArrayField(BINARY_COLUMN)
          .build();

  public static final Schema TEST_SCHEMA =
      Schema.builder().addStringField(KEY).addRowField(FAMILY_TEST, TEST_FAMILY_SCHEMA).build();

  public static final Schema TEST_FLAT_SCHEMA =
      Schema.builder()
          .addStringField(KEY)
          .addBooleanField(BOOL_COLUMN)
          .addInt64Field(LONG_COLUMN)
          .addStringField(STRING_COLUMN)
          .addDoubleField(DOUBLE_COLUMN)
          .build();

  public static final long NOW = Instant.now().toEpochMilli() * 1_000;
  public static final long LATER = NOW + 1_000;

  public static byte[] doubleToByteArray(double number) {
    return Longs.toByteArray(Double.doubleToLongBits(number));
  }

  public static byte[] booleanToByteArray(boolean condition) {
    return condition ? new byte[] {1} : new byte[] {0};
  }

  public static void checkMessage(@Nullable String message, String substring) {
    if (message != null) {
      assertThat(message, containsString(substring));
    } else {
      fail();
    }
  }
}
