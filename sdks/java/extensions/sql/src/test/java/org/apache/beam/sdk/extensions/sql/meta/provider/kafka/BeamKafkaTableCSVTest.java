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
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

public class BeamKafkaTableCSVTest extends BeamKafkaTableTest {
  @Override
  protected KafkaTestTable getTable(int numberOfPartitions) {
    return new KafkaTestTableCSV(BEAM_SQL_SCHEMA, TOPICS, numberOfPartitions);
  }

  @Override
  protected KafkaTestRecord<?> createKafkaTestRecord(
      String key, List<Object> values, long timestamp) {
    StringBuilder csv = new StringBuilder();
    values.forEach(value -> csv.append(value).append(","));
    csv.deleteCharAt(csv.length() - 1);
    csv.trimToSize();
    return KafkaTestRecord.create(key, csv.toString(), "topic1", timestamp);
  }

  @Override
  protected PCollection<Row> createRecorderDecoder(TestPipeline pipeline) {
    return pipeline
        .apply(Create.of("1,\"1\",1.0", "2,2,2.0"))
        .apply(ParDo.of(new String2KvBytes()))
        .apply(new BeamKafkaCSVTable.CsvRecorderDecoder(SCHEMA, CSVFormat.DEFAULT));
  }

  @Override
  protected PCollection<Row> createRecorderEncoder(TestPipeline pipeline) {
    return pipeline
        .apply(Create.of(ROW1, ROW2))
        .apply(new BeamKafkaCSVTable.CsvRecorderEncoder(CSVFormat.DEFAULT))
        .apply(new BeamKafkaCSVTable.CsvRecorderDecoder(SCHEMA, CSVFormat.DEFAULT));
  }
}
