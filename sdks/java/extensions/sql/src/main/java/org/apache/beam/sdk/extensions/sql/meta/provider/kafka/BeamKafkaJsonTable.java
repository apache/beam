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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BeamKafkaJsonTable extends BeamKafkaTable {
  public BeamKafkaJsonTable(Schema beamSchema, String bootstrapServers, List<String> topics) {
    super(beamSchema, bootstrapServers, topics);
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    ObjectMapper objectMapper =
        RowJsonUtils.newObjectMapperWith(RowJson.RowJsonDeserializer.forSchema(schema));
    return new BeamKafkaJsonTable.JsonRecorderDecoder(schema, objectMapper);
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> getPTransformForOutput() {
    ObjectMapper objectMapper =
        RowJsonUtils.newObjectMapperWith(RowJson.RowJsonSerializer.forSchema(schema));
    return new BeamKafkaJsonTable.JsonRecorderEncoder(objectMapper);
  }

  /** A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}. */
  private static class JsonRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {
    private final Schema schema;
    private final ObjectMapper objectMapper;

    public JsonRecorderDecoder(Schema schema, ObjectMapper objectMapper) {
      this.schema = schema;
      this.objectMapper = objectMapper;
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      return input
          .apply(
              "decodeJsonRecord",
              ParDo.of(
                  new DoFn<KV<byte[], byte[]>, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      String rowInString = new String(c.element().getValue(), UTF_8);
                      Row row = RowJsonUtils.jsonToRow(objectMapper, rowInString);
                      c.output(row);
                    }
                  }))
          .setRowSchema(schema);
    }
  }

  /** A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}. */
  private static class JsonRecorderEncoder
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {
    private final ObjectMapper objectMapper;

    public JsonRecorderEncoder(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      return input.apply(
          "encodeJsonRecord",
          ParDo.of(
              new DoFn<Row, KV<byte[], byte[]>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(
                      KV.of(
                          new byte[] {},
                          RowJsonUtils.rowToJson(objectMapper, c.element()).getBytes(UTF_8)));
                }
              }));
    }
  }
}
