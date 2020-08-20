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
package org.apache.beam.sdk.io.gcp.spanner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.beamRowToMutationFn;
import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;

public class MutationUtilsTest {
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Schema INT64_SCHEMA = Schema.builder().addInt64Field("int64").build();
  private static final Row EMPTY_ROW = Row.withSchema(EMPTY_SCHEMA).build();
  private static final Row INT64_ROW =
      Row.withSchema(INT64_SCHEMA).withFieldValue("int64", 3L).build();
  private static final Struct EMPTY_STRUCT = Struct.newBuilder().build();
  private static final Struct INT64_STRUCT = Struct.newBuilder().set("int64").to(3L).build();
  private static final String TABLE = "some_table";

  private static final Schema WRITE_ROW_SCHEMA =
      Schema.builder()
          .addInt64Field("f_int64")
          .addDoubleField("f_float64")
          .addStringField("f_string")
          .addByteArrayField("f_bytes")
          .addDateTimeField("f_date_time")
          .addBooleanField("f_bool")
          .addRowField("f_struct", EMPTY_SCHEMA)
          .addRowField("f_struct_int64", INT64_SCHEMA)
          .addArrayField("f_array", Schema.FieldType.INT64)
          .addArrayField("f_struct_array", Schema.FieldType.row(INT64_SCHEMA))
          .build();

  private static final Row WRITE_ROW =
      Row.withSchema(WRITE_ROW_SCHEMA)
          .withFieldValue("f_int64", 1L)
          .withFieldValue("f_float64", 1.1)
          .withFieldValue("f_string", "donald_duck")
          .withFieldValue("f_bytes", "some_bytes".getBytes(UTF_8))
          .withFieldValue("f_date_time", DateTime.parse("2077-10-15T00:00:00+00:00"))
          .withFieldValue("f_bool", false)
          .withFieldValue("f_struct", EMPTY_ROW)
          .withFieldValue("f_struct_int64", INT64_ROW)
          .withFieldValue("f_array", ImmutableList.of(2L, 3L))
          .withFieldValue("f_struct_array", ImmutableList.of(INT64_ROW, INT64_ROW))
          .build();

  private static final Schema KEY_SCHEMA =
      Schema.builder()
          .addInt64Field("f_int64")
          .addDoubleField("f_float64")
          .addStringField("f_string")
          .addByteArrayField("f_bytes")
          .addDateTimeField("f_date_time")
          .addBooleanField("f_bool")
          .build();

  private static final Row KEY_ROW =
      Row.withSchema(KEY_SCHEMA)
          .withFieldValue("f_int64", 1L)
          .withFieldValue("f_float64", 1.1)
          .withFieldValue("f_string", "donald_duck")
          .withFieldValue("f_bytes", "some_bytes".getBytes(UTF_8))
          .withFieldValue("f_date_time", DateTime.parse("2077-10-15T00:00:00+00:00"))
          .withFieldValue("f_bool", false)
          .build();

  private static final Schema WRITE_MUTATION_SCHEMA =
      Schema.builder()
          .addStringField("operation")
          .addStringField("table")
          .addNullableField("row", Schema.FieldType.row(WRITE_ROW_SCHEMA))
          .build();

  private static final Schema DELETE_MUTATION_SCHEMA =
      Schema.builder()
          .addStringField("operation")
          .addStringField("table")
          .addNullableField("keyset", Schema.FieldType.array(Schema.FieldType.row(KEY_SCHEMA)))
          .build();

  @Test
  public void testCreateInsertMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.INSERT);

    Row mutationRow = createWriteMutationRow(Mutation.Op.INSERT);

    Mutation mutation = beamRowToMutationFn().apply(mutationRow);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateUpdateMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.UPDATE);

    Row mutationRow = createWriteMutationRow(Mutation.Op.UPDATE);

    Mutation mutation = beamRowToMutationFn().apply(mutationRow);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateReplaceMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.REPLACE);

    Row mutationRow = createWriteMutationRow(Mutation.Op.REPLACE);

    Mutation mutation = beamRowToMutationFn().apply(mutationRow);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateInsertOrUpdateMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.INSERT_OR_UPDATE);

    Row mutationRow = createWriteMutationRow(Mutation.Op.INSERT_OR_UPDATE);

    Mutation mutation = beamRowToMutationFn().apply(mutationRow);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateDeleteMutationFromRow() {
    Mutation expectedMutation = createDeleteMutation();

    Row mutationRow =
        Row.withSchema(DELETE_MUTATION_SCHEMA)
            .withFieldValue("table", TABLE)
            .withFieldValue("operation", Mutation.Op.DELETE.toString())
            .withFieldValue("keyset", ImmutableList.of(KEY_ROW, KEY_ROW))
            .build();

    Mutation mutation = beamRowToMutationFn().apply(mutationRow);
    assertEquals(expectedMutation, mutation);
  }

  private Row createWriteMutationRow(Mutation.Op op) {
    return Row.withSchema(WRITE_MUTATION_SCHEMA)
        .withFieldValue("table", TABLE)
        .withFieldValue("operation", op.toString())
        .withFieldValue("row", WRITE_ROW)
        .build();
  }

  private static Mutation createDeleteMutation() {
    Key key =
        Key.newBuilder()
            .append(1L)
            .append(1.1)
            .append("donald_duck")
            .append(ByteArray.copyFrom("some_bytes".getBytes(UTF_8)))
            .append(Timestamp.parseTimestamp("2077-10-15T00:00:00"))
            .append(false)
            .build();
    return Mutation.delete(TABLE, KeySet.newBuilder().addKey(key).addKey(key).build());
  }

  private static Mutation createMutation(Mutation.Op operation) {
    Mutation.WriteBuilder builder = chooseBuilder(operation);
    return builder
        .set("f_int64")
        .to(1L)
        .set("f_float64")
        .to(1.1)
        .set("f_string")
        .to("donald_duck")
        .set("f_bytes")
        .to(ByteArray.copyFrom("some_bytes".getBytes(UTF_8)))
        .set("f_date_time")
        .to(Timestamp.parseTimestamp("2077-10-15T00:00:00"))
        .set("f_bool")
        .to(false)
        .set("f_struct")
        .to(EMPTY_STRUCT)
        .set("f_struct_int64")
        .to(Struct.newBuilder().set("int64").to(3L).build())
        .set("f_array")
        .toInt64Array(ImmutableList.of(2L, 3L))
        .set("f_struct_array")
        .toStructArray(
            Type.struct(ImmutableList.of(Type.StructField.of("int64", Type.int64()))),
            ImmutableList.of(INT64_STRUCT, INT64_STRUCT))
        .build();
  }

  private static Mutation.WriteBuilder chooseBuilder(Mutation.Op op) {
    switch (op) {
      case INSERT:
        return Mutation.newInsertBuilder(TABLE);
      case UPDATE:
        return Mutation.newUpdateBuilder(TABLE);
      case REPLACE:
        return Mutation.newReplaceBuilder(TABLE);
      case INSERT_OR_UPDATE:
        return Mutation.newInsertOrUpdateBuilder(TABLE);
      default:
        return null;
    }
  }
}
