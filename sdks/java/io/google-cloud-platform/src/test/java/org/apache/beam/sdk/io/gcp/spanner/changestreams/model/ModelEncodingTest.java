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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Test;

public class ModelEncodingTest {

  @Test
  public void testModCanBeEncoded() throws IOException {
    final Mod mod =
        new Mod(
            "{\"column1\": \"value1\"}",
            "{\"column2\": \"oldValue2\"}",
            "{\"column2\": \"newValue2\"}");

    assertEquals(mod, encodeAndDecode(mod));
  }

  @Test
  public void testModTypeCanBeEncoded() throws IOException {
    assertEquals(ModType.INSERT, encodeAndDecode(ModType.INSERT));
  }

  @Test
  public void testTypeCodeCanBeEncoded() throws IOException {
    final TypeCode typeCode = new TypeCode("typeCode");

    assertEquals(typeCode, encodeAndDecode(typeCode));
  }

  @Test
  public void testValueCaptureTypeCanBeEncoded() throws IOException {
    assertEquals(
        ValueCaptureType.OLD_AND_NEW_VALUES, encodeAndDecode(ValueCaptureType.OLD_AND_NEW_VALUES));
  }

  @Test
  public void testColumnTypeCanBeEncoded() throws IOException {
    final ColumnType columnType = new ColumnType("column", new TypeCode("typeCode"), true, 1);

    assertEquals(columnType, encodeAndDecode(columnType));
  }

  @Test
  public void testDataChangeRecordCanBeEncoded() throws IOException {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "1",
            Timestamp.now(),
            "2",
            true,
            "3",
            "TableName",
            Arrays.asList(
                new ColumnType("keyColumn", new TypeCode("typeKey"), true, 1),
                new ColumnType("column1", new TypeCode("typeCode1"), false, 2),
                new ColumnType("column2", new TypeCode("typeCode2"), false, 3)),
            Collections.singletonList(
                new Mod(
                    "{\"keyColumn\": \"keyValue\"}",
                    "{\"column1\": \"value1\", \"column2\": \"oldValue2\"}",
                    "{\"column1\": \"value1\", \"column2\": \"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1L,
            1L,
            "transactionTag",
            true,
            ChangeStreamRecordMetadata.newBuilder()
                .withRecordTimestamp(Timestamp.ofTimeMicroseconds(100L))
                .withPartitionToken("1")
                .withPartitionStartTimestamp(Timestamp.ofTimeMicroseconds(1L))
                .withPartitionEndTimestamp(Timestamp.ofTimeMicroseconds(10_000L))
                .withPartitionCreatedAt(Timestamp.ofTimeMicroseconds(100L))
                .withPartitionScheduledAt(Timestamp.ofTimeMicroseconds(101L))
                .withPartitionRunningAt(Timestamp.ofTimeMicroseconds(102L))
                .withQueryStartedAt(Timestamp.ofTimeMicroseconds(103L))
                .withRecordStreamStartedAt(Timestamp.ofTimeMicroseconds(104L))
                .withRecordStreamEndedAt(Timestamp.ofTimeMicroseconds(105L))
                .withRecordReadAt(Timestamp.ofTimeMicroseconds(106L))
                .withTotalStreamTimeMillis(1_000L)
                .withNumberOfRecordsRead(1L)
                .build());

    assertEquals(dataChangeRecord, encodeAndDecode(dataChangeRecord));
  }

  @Test
  public void testDataChangeRecordWithNullOldAndNewValuesAndNullMetadataCanBeEncoded()
      throws IOException {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "1",
            Timestamp.now(),
            "2",
            true,
            "3",
            "TableName",
            Arrays.asList(
                new ColumnType("keyColumn", new TypeCode("typeKey"), true, 1),
                new ColumnType("column1", new TypeCode("typeCode1"), false, 2),
                new ColumnType("column2", new TypeCode("typeCode2"), false, 3)),
            Collections.singletonList(new Mod("{\"keyColumn\": \"keyValue\"}", null, null)),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1L,
            1L,
            "transactionTag",
            true,
            null);

    assertEquals(dataChangeRecord, encodeAndDecode(dataChangeRecord));
  }

  @Test
  public void testPartitionMetadataCanBeEncoded() throws IOException {
    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(
            "partitionToken",
            Sets.newHashSet("parentToken"),
            Timestamp.now(),
            Timestamp.now(),
            10,
            State.RUNNING,
            Timestamp.now(),
            Timestamp.now(),
            Timestamp.now(),
            Timestamp.now(),
            Timestamp.now());

    assertEquals(partitionMetadata, encodeAndDecode(partitionMetadata));
  }

  private <T> Object encodeAndDecode(T object) throws IOException {
    final Schema schema = ReflectData.get().getSchema(object.getClass());
    final ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write(object, encoder);
    encoder.flush();

    final ReflectDatumReader<Object> datumReader = new ReflectDatumReader<>(schema);
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    return datumReader.read(null, decoder);
  }
}
