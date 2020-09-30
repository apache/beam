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

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class BeamKafkaTableAvroTest extends BeamKafkaTableTest {
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();

  private final SerializableFunction<Row, byte[]> toBytesFn =
      AvroUtils.getRowToAvroBytesFunction(getSchema());

  @Override
  protected Schema getSchema() {
    return Schema.builder()
        .addInt64Field("f_long")
        .addInt32Field("f_int")
        .addDoubleField("f_double")
        .addStringField("f_string")
        .addBooleanField("f_bool")
        .addRowField("f_row", EMPTY_SCHEMA)
        .addArrayField("f_array", Schema.FieldType.row(EMPTY_SCHEMA))
        .build();
  }

  @Override
  protected List<Object> listFrom(int i) {
    return ImmutableList.of(
        (long) i,
        i,
        (double) i,
        "avro_value" + i,
        i % 2 == 0,
        Row.withSchema(EMPTY_SCHEMA).build(),
        ImmutableList.of(Row.withSchema(EMPTY_SCHEMA).build()));
  }

  @Override
  protected KafkaTestTable getTestTable(int numberOfPartitions) {
    return new KafkaTestTableAvro(getSchema(), TOPICS, numberOfPartitions);
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return new BeamKafkaAvroTable(getSchema(), "", ImmutableList.of());
  }

  @Override
  protected KafkaTestRecord<?> createKafkaTestRecord(String key, int i, long timestamp) {
    Row row = generateRow(i);
    return KafkaTestRecord.create(key, toBytesFn.apply(row), "topic1", timestamp);
  }

  @Override
  protected PCollection<KV<byte[], byte[]>> applyRowToBytesKV(PCollection<Row> rows) {
    return rows.apply(MapElements.via(new RowToBytesKV(toBytesFn)));
  }

  private static class RowToBytesKV extends SimpleFunction<Row, KV<byte[], byte[]>> {
    private final SerializableFunction<Row, byte[]> toBytesFn;

    RowToBytesKV(SerializableFunction<Row, byte[]> toBytesFn) {
      this.toBytesFn = toBytesFn;
    }

    @Override
    public KV<byte[], byte[]> apply(Row row) {
      return KV.of(new byte[] {}, toBytesFn.apply(row));
    }
  }
}
