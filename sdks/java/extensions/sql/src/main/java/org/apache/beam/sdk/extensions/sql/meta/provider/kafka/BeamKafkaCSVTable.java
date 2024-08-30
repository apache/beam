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
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.csvLines2BeamRows;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.clients.producer.ProducerRecord;

/** A Kafka topic that saves records as CSV format. */
public class BeamKafkaCSVTable extends BeamKafkaTable {
  private final CSVFormat csvFormat;

  public BeamKafkaCSVTable(Schema beamSchema, String bootstrapServers, List<String> topics) {
    this(beamSchema, bootstrapServers, topics, CSVFormat.DEFAULT);
  }

  public BeamKafkaCSVTable(
      Schema beamSchema, String bootstrapServers, List<String> topics, CSVFormat format) {
    super(beamSchema, bootstrapServers, topics);
    this.csvFormat = format;
  }

  @Override
  protected PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput() {
    return new CsvRecorderDecoder(schema, csvFormat);
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>
      getPTransformForOutput() {
    return new CsvRecorderEncoder(csvFormat, Iterables.getOnlyElement(getTopics()));
  }

  /** A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}. */
  private static class CsvRecorderDecoder
      extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>> {
    private final Schema schema;
    private final CSVFormat format;

    CsvRecorderDecoder(Schema schema, CSVFormat format) {
      this.schema = schema;
      this.format = format;
    }

    @Override
    public PCollection<Row> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
      return input
          .apply(
              "decodeCsvRecord",
              ParDo.of(
                  new DoFn<KafkaRecord<byte[], byte[]>, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<byte[], byte[]> kv = checkArgumentNotNull(c.element()).getKV();
                      String rowInString = new String(checkArgumentNotNull(kv.getValue()), UTF_8);
                      for (Row row : csvLines2BeamRows(format, rowInString, schema)) {
                        c.output(row);
                      }
                    }
                  }))
          .setRowSchema(schema);
    }
  }

  /** A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}. */
  private static class CsvRecorderEncoder
      extends PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>> {
    private final CSVFormat format;
    private final String topic;

    CsvRecorderEncoder(CSVFormat format, String topic) {
      this.format = format;
      this.topic = topic;
    }

    @Override
    public PCollection<ProducerRecord<byte[], byte[]>> expand(PCollection<Row> input) {
      return input.apply(
          "encodeCsvRecord",
          ParDo.of(
              new DoFn<Row, ProducerRecord<byte[], byte[]>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  Row in = checkArgumentNotNull(c.element());
                  c.output(
                      new ProducerRecord<>(
                          topic, new byte[] {}, beamRow2CsvLine(in, format).getBytes(UTF_8)));
                }
              }));
    }
  }
}
