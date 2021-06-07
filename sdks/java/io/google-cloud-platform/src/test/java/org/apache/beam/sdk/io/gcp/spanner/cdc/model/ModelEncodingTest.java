/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.junit.Test;

public class ModelEncodingTest {

  @Test
  public void testModCanBeEncoded() throws IOException {
    final Mod mod = new Mod(
        ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
        ImmutableMap.of("column1", "value1", "column2", "newValue2")
    );

    assertEquals(mod, encodeAndDecode(mod));
  }

  @Test
  public void testModTypeCanBeEncoded() throws IOException {
    assertEquals(ModType.INSERT, encodeAndDecode(ModType.INSERT));
  }

  @Test
  public void testRecordSequenceCanBeEncoded() throws IOException {
    final RecordSequence recordSequence = new RecordSequence(2L);

    assertEquals(recordSequence, encodeAndDecode(recordSequence));
  }

  @Test
  public void testTypeCodeCanBeEncoded() throws IOException {
    final TypeCode typeCode = new TypeCode("typeCode");

    assertEquals(typeCode, encodeAndDecode(typeCode));
  }

  @Test
  public void testValueCaptureTypeCanBeEncoded() throws IOException {
    assertEquals(ValueCaptureType.OLD_AND_NEW_VALUES, encodeAndDecode(ValueCaptureType.OLD_AND_NEW_VALUES));
  }

  @Test
  public void testColumnTypeCanBeEncoded() throws IOException {
    final ColumnType columnType = new ColumnType("column", new TypeCode("typeCode"), true);

    assertEquals(columnType, encodeAndDecode(columnType));
  }

  @Test
  public void testDataChangesRecordCanBeEncoded() throws IOException {
    final DataChangesRecord dataChangesRecord = new DataChangesRecord(
        "1",
        Timestamp.now(),
        "2",
        true,
        "3",
        "TableName",
        Arrays.asList(
            new ColumnType("column1", new TypeCode("typeCode1"), true),
            new ColumnType("column2", new TypeCode("typeCode2"), false)
        ),
        Collections.singletonList(
            new Mod(
                ImmutableMap.of("column1", "value1", "column2", "oldValue2"),
                ImmutableMap.of("column1", "value1", "column2", "newValue2")
            )
        ),
        ModType.INSERT,
        ValueCaptureType.OLD_AND_NEW_VALUES
    );

    assertEquals(dataChangesRecord, encodeAndDecode(dataChangesRecord));
  }

  @Test
  public void testPartitionMetadataCanBeEncoded() throws IOException {
    final PartitionMetadata partitionMetadata = new PartitionMetadata(
        "partitionToken",
        ImmutableList.of("parentToken"),
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
