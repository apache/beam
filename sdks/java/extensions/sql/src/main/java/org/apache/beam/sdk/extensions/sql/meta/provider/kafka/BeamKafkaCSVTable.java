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

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.csvLine2BeamRow;

import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.commons.csv.CSVFormat;

/**
 * A Kafka topic that saves records as CSV format.
 *
 */
public class BeamKafkaCSVTable extends BeamKafkaTable {
  private CSVFormat csvFormat;
  public BeamKafkaCSVTable(RowType beamRowType, String bootstrapServers,
                           List<String> topics) {
    this(beamRowType, bootstrapServers, topics, CSVFormat.DEFAULT);
  }

  public BeamKafkaCSVTable(RowType beamRowType, String bootstrapServers,
                           List<String> topics, CSVFormat format) {
    super(beamRowType, bootstrapServers, topics);
    this.csvFormat = format;
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput() {
    return new CsvRecorderDecoder(rowType, csvFormat);
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput() {
    return new CsvRecorderEncoder(rowType, csvFormat);
  }

  /**
   * A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}.
   *
   */
  public static class CsvRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {
    private RowType rowType;
    private CSVFormat format;
    public CsvRecorderDecoder(RowType rowType, CSVFormat format) {
      this.rowType = rowType;
      this.format = format;
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      return input.apply("decodeRecord", ParDo.of(new DoFn<KV<byte[], byte[]>, Row>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          String rowInString = new String(c.element().getValue());
          c.output(csvLine2BeamRow(format, rowInString, rowType));
        }
      }));
    }
  }

  /**
   * A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}.
   *
   */
  public static class CsvRecorderEncoder
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {
    private RowType rowType;
    private CSVFormat format;
    public CsvRecorderEncoder(RowType rowType, CSVFormat format) {
      this.rowType = rowType;
      this.format = format;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      return input.apply("encodeRecord", ParDo.of(new DoFn<Row, KV<byte[], byte[]>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          Row in = c.element();
          c.output(KV.of(new byte[] {}, beamRow2CsvLine(in, format).getBytes()));
        }
      }));
    }
  }
}
