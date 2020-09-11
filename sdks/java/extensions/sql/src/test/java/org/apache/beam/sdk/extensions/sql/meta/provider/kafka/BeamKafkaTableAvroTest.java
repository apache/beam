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

import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BeamKafkaTableAvroTest extends BeamKafkaTableTest {
  @Override
  protected KafkaTestTable getTable(int numberOfPartitions) {
    return new KafkaTestTableAvro(BEAM_SQL_SCHEMA, TOPICS, numberOfPartitions);
  }

  @Override
  protected KafkaTestRecord<?> createKafkaTestRecord(
      String key, boolean useFixedKey, int i, int timestamp, boolean useFixedTimestamp) {
    key = useFixedKey ? key : key + i;
    timestamp = useFixedTimestamp ? timestamp : timestamp * i;
    Row row = Row.withSchema(SCHEMA).attachValues((long) i, 1, 2.0d);
    return KafkaTestRecord.create(key, AvroUtils.rowToAvroBytes(row), "topic1", timestamp);
  }

  @Override
  protected PCollection<Row> createRecorderDecoder(TestPipeline pipeline) {
    return pipeline
        .apply(Create.of(ROW1, ROW2))
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(byte[].class), TypeDescriptor.of(byte[].class)))
                .via(row -> KV.of(new byte[] {}, AvroUtils.rowToAvroBytes(row))))
        .apply(new BeamKafkaAvroTable.AvroRecorderDecoder(SCHEMA));
  }

  @Override
  protected PCollection<Row> createRecorderEncoder(TestPipeline pipeline) {
    return pipeline
        .apply(Create.of(ROW1, ROW2))
        .apply(new BeamKafkaAvroTable.AvroRecorderEncoder())
        .apply(new BeamKafkaAvroTable.AvroRecorderDecoder(SCHEMA));
  }
}
