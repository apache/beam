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

import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka topic that saves records as CSV format.
 *
 */
public class BeamKafkaCSVTable extends BeamKafkaTable {

  /**
   *
   */
  private static final long serialVersionUID = 4754022536543333984L;

  public static final String DELIMITER = ",";
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaCSVTable.class);

  public BeamKafkaCSVTable(RelProtoDataType protoRowType, String bootstrapServers,
      List<String> topics) {
    super(protoRowType, bootstrapServers, topics);
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>>
      getPTransformForInput() {
    return new CsvRecorderDecoder(beamSqlRecordType);
  }

  @Override
  public PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>>
      getPTransformForOutput() {
    return new CsvRecorderEncoder(beamSqlRecordType);
  }

  /**
   * A PTransform to convert {@code KV<byte[], byte[]>} to {@link BeamSQLRow}.
   *
   */
  public static class CsvRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>> {
    private BeamSQLRecordType recordType;

    public CsvRecorderDecoder(BeamSQLRecordType recordType) {
      this.recordType = recordType;
    }

    @Override
    public PCollection<BeamSQLRow> expand(PCollection<KV<byte[], byte[]>> input) {
      return input.apply("decodeRecord", ParDo.of(new DoFn<KV<byte[], byte[]>, BeamSQLRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          String rowInString = new String(c.element().getValue());
          String[] parts = rowInString.split(BeamKafkaCSVTable.DELIMITER);
          if (parts.length != recordType.size()) {
            LOG.error(String.format("invalid record: ", rowInString));
          } else {
            BeamSQLRow sourceRecord = new BeamSQLRow(recordType);
            for (int idx = 0; idx < parts.length; ++idx) {
              sourceRecord.addField(idx, parts[idx]);
            }
            c.output(sourceRecord);
          }
        }
      }));
    }
  }

  /**
   * A PTransform to convert {@link BeamSQLRow} to {@code KV<byte[], byte[]>}.
   *
   */
  public static class CsvRecorderEncoder
      extends PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>> {
    private BeamSQLRecordType recordType;

    public CsvRecorderEncoder(BeamSQLRecordType recordType) {
      this.recordType = recordType;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<BeamSQLRow> input) {
      return input.apply("encodeRecord", ParDo.of(new DoFn<BeamSQLRow, KV<byte[], byte[]>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          BeamSQLRow in = c.element();
          StringBuffer sb = new StringBuffer();
          for (int idx = 0; idx < in.size(); ++idx) {
            sb.append(DELIMITER);
            sb.append(in.getFieldValue(idx).toString());
          }
          c.output(KV.of(new byte[] {}, sb.substring(1).getBytes()));
        }
      }));

    }

  }

}
