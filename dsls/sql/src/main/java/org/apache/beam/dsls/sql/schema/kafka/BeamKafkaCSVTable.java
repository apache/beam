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
package org.apache.beam.dsls.sql.schema.kafka;

import static org.apache.beam.dsls.sql.schema.BeamTableUtils.beamSQLRow2CsvLine;
import static org.apache.beam.dsls.sql.schema.BeamTableUtils.csvLine2BeamSQLRow;

import java.util.List;

import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.commons.csv.CSVFormat;

/**
 * A Kafka topic that saves records as CSV format.
 *
 */
public class BeamKafkaCSVTable extends BeamKafkaTable {
  private CSVFormat csvFormat;
  public BeamKafkaCSVTable(RelProtoDataType protoRowType, String bootstrapServers,
      List<String> topics) {
    this(protoRowType, bootstrapServers, topics, CSVFormat.DEFAULT);
  }

  public BeamKafkaCSVTable(RelProtoDataType protoRowType, String bootstrapServers,
      List<String> topics, CSVFormat format) {
    super(protoRowType, bootstrapServers, topics);
    this.csvFormat = format;
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSqlRow>>
      getPTransformForInput() {
    return new CsvRecorderDecoder(beamSqlRecordType, csvFormat);
  }

  @Override
  public PTransform<PCollection<BeamSqlRow>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput() {
    return new CsvRecorderEncoder(beamSqlRecordType, csvFormat);
  }

  /**
   * A PTransform to convert {@code KV<byte[], byte[]>} to {@link BeamSqlRow}.
   *
   */
  public static class CsvRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSqlRow>> {
    private BeamSqlRecordType recordType;
    private CSVFormat format;
    public CsvRecorderDecoder(BeamSqlRecordType recordType, CSVFormat format) {
      this.recordType = recordType;
      this.format = format;
    }

    @Override
    public PCollection<BeamSqlRow> expand(PCollection<KV<byte[], byte[]>> input) {
      return input.apply("decodeRecord", ParDo.of(new DoFn<KV<byte[], byte[]>, BeamSqlRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          String rowInString = new String(c.element().getValue());
          c.output(csvLine2BeamSQLRow(format, rowInString, recordType));
        }
      }));
    }
  }

  /**
   * A PTransform to convert {@link BeamSqlRow} to {@code KV<byte[], byte[]>}.
   *
   */
  public static class CsvRecorderEncoder
      extends PTransform<PCollection<BeamSqlRow>, PCollection<KV<byte[], byte[]>>> {
    private BeamSqlRecordType recordType;
    private CSVFormat format;
    public CsvRecorderEncoder(BeamSqlRecordType recordType, CSVFormat format) {
      this.recordType = recordType;
      this.format = format;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<BeamSqlRow> input) {
      return input.apply("encodeRecord", ParDo.of(new DoFn<BeamSqlRow, KV<byte[], byte[]>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          BeamSqlRow in = c.element();
          c.output(KV.of(new byte[] {}, beamSQLRow2CsvLine(in, format).getBytes()));
        }
      }));
    }
  }
}
