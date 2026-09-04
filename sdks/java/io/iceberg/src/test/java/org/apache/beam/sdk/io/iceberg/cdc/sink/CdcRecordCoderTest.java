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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CdcRecordCoder}. */
@RunWith(JUnit4.class)
public class CdcRecordCoderTest {

  private static final Schema DATA_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").addStringField("data").build();

  @Test
  public void roundTripsAllValueKinds() throws Exception {
    for (ValueKind kind : ValueKind.values()) {
      CdcRecord record =
          CdcRecord.of(Row.withSchema(DATA_SCHEMA).addValues(1, "a", "x").build(), kind, 12L);
      CoderProperties.coderDecodeEncodeEqual(CdcRecordCoder.of(DATA_SCHEMA), record);
    }
  }

  /** The coder's own properties: deterministic, serializable, schema-keyed equality. */
  @Test
  public void coderIsDeterministicSerializableAndSchemaKeyed() throws Exception {
    CdcRecordCoder.of(DATA_SCHEMA).verifyDeterministic();
    CoderProperties.coderSerializable(CdcRecordCoder.of(DATA_SCHEMA));
    // decode() rebuilds the row with the coder's schema object, which may differ from the
    // original row's, so the coder must not claim consistency with equals.
    assertFalse(CdcRecordCoder.of(DATA_SCHEMA).consistentWithEquals());
    assertThat(CdcRecordCoder.of(DATA_SCHEMA).getDataSchema(), Matchers.equalTo(DATA_SCHEMA));

    Schema otherSchema = Schema.builder().addInt32Field("other").build();
    assertThat(CdcRecordCoder.of(DATA_SCHEMA), Matchers.equalTo(CdcRecordCoder.of(DATA_SCHEMA)));
    assertThat(
        CdcRecordCoder.of(DATA_SCHEMA).hashCode(),
        Matchers.equalTo(CdcRecordCoder.of(DATA_SCHEMA).hashCode()));
    assertThat(
        CdcRecordCoder.of(DATA_SCHEMA),
        Matchers.not(Matchers.equalTo(CdcRecordCoder.of(otherSchema))));
  }

  @Test
  public void encodePinnedWireMapping() throws Exception {
    // The stream layout is [row bytes][kind VarInt][seq VarLong]. Codes 0-3 each fit in a single
    // VarInt byte equal to the code itself, so the byte immediately following the row bytes must
    // be the pinned code for that ValueKind.
    Row row = Row.withSchema(DATA_SCHEMA).addValues(1, "a", "x").build();
    ByteArrayOutputStream rowOnly = new ByteArrayOutputStream();
    RowCoder.of(DATA_SCHEMA).encode(row, rowOnly);
    int rowLen = rowOnly.toByteArray().length;

    assertPinnedKindCode(row, ValueKind.INSERT, rowLen, 1);
    assertPinnedKindCode(row, ValueKind.UPDATE_BEFORE, rowLen, 2);
    assertPinnedKindCode(row, ValueKind.UPDATE_AFTER, rowLen, 3);
    assertPinnedKindCode(row, ValueKind.DELETE, rowLen, 4);
  }

  private static void assertPinnedKindCode(Row row, ValueKind kind, int rowLen, int expectedCode)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    CdcRecordCoder.of(DATA_SCHEMA).encode(CdcRecord.of(row, kind, 1L), out);
    byte[] bytes = out.toByteArray();
    assertThat(bytes[rowLen] & 0xFF, Matchers.equalTo(expectedCode));
  }

  @Test
  public void decodeRejectsUnknownKindCode() throws Exception {
    // Hand-encode a stream with a valid data row and seq, but a ValueKind code (4) at the pinned
    // mapping's boundary: one past the highest valid code (3).
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RowCoder.of(DATA_SCHEMA)
        .encode(Row.withSchema(DATA_SCHEMA).addValues(1, "a", "x").build(), out);
    VarIntCoder.of().encode(5, out);
    VarLongCoder.of().encode(1L, out);

    CdcRecordCoder coder = CdcRecordCoder.of(DATA_SCHEMA);
    assertThrows(
        CoderException.class, () -> coder.decode(new ByteArrayInputStream(out.toByteArray())));
  }
}
