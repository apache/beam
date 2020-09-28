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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.csv.CSVFormat;

public class BeamKafkaTableCSVTest extends BeamKafkaTableTest {
  private final Schema SCHEMA =
      Schema.builder()
          .addInt64Field("f_long")
          .addInt32Field("f_int")
          .addInt16Field("f_short")
          .addByteField("f_byte")
          .addDoubleField("f_double")
          .addStringField("f_string")
          .build();

  @Override
  protected Schema getSchema() {
    return SCHEMA;
  }

  @Override
  protected List<Object> listFrom(int i) {
    return ImmutableList.of((long) i, i, (short) i, (byte) i, (double) i, "csv_value" + i);
  }

  @Override
  protected KafkaTestTable getTestTable(int numberOfPartitions) {
    return new KafkaTestTableCSV(getSchema(), TOPICS, numberOfPartitions);
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return new BeamKafkaCSVTable(getSchema(), "", ImmutableList.of());
  }

  @Override
  protected KafkaTestRecord<?> createKafkaTestRecord(String key, int i, long timestamp) {
    String csv = beamRow2CsvLine(generateRow(i), CSVFormat.DEFAULT);
    return KafkaTestRecord.create(key, csv, "topic1", timestamp);
  }

  @Override
  protected PCollection<KV<byte[], byte[]>> applyRowToBytesKV(PCollection<Row> rows) {
    return rows.apply(
        MapElements.into(
                TypeDescriptors.kvs(
                    TypeDescriptor.of(byte[].class), TypeDescriptor.of(byte[].class)))
            .via(
                row ->
                    KV.of(new byte[] {}, beamRow2CsvLine(row, CSVFormat.DEFAULT).getBytes(UTF_8))));
  }
}
