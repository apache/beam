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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class ModelEncodingTest {

  @Test
  public void testModCanBeEncoded() throws IOException {
    final Mod mod =
        new Mod(
            ImmutableMap.of("keyColumn1", "keyValue1"),
            ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
            ImmutableMap.of("column1", "value1", "column2", "newValue2"));

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
                    ImmutableMap.of("keyColumn", "keyValue"),
                    ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
                    ImmutableMap.of("column1", "value1", "column2", "newValue2"))),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1,
            1);

    assertEquals(dataChangeRecord, encodeAndDecode(dataChangeRecord));
  }

  @Test
  public void testDataChangeRecordWithNullOldAndNewValuesCanBeEncoded() throws IOException {
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
                new Mod(ImmutableMap.of("keyColumn", "keyValue"), null, null)),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1,
            1);

    assertEquals(dataChangeRecord, encodeAndDecode(dataChangeRecord));
  }

  @Test
  public void testDataChangeRecordWithNullValuesInsideAModCanBeEncoded() throws IOException {
    final Map<String, String> oldValues = new HashMap<>();
    oldValues.put("column1", "oldValue1");
    oldValues.put("column2", null);
    final Map<String, String> newValues = new HashMap<>();
    newValues.put("column1", null);
    newValues.put("column2", "newValue2");
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
                new Mod(ImmutableMap.of("keyColumn", "keyValue"), oldValues, newValues)),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1,
            1);

    assertEquals(dataChangeRecord, encodeAndDecode(dataChangeRecord));
  }

  @Test
  public void testPartitionMetadataCanBeEncoded() throws IOException {
    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(
            "partitionToken",
            Sets.newHashSet("parentToken"),
            Timestamp.now(),
            true,
            Timestamp.now(),
            false,
            10,
            State.CREATED,
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
