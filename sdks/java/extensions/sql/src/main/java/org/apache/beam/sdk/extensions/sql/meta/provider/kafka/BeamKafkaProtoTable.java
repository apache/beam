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

import com.google.protobuf.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class BeamKafkaProtoTable<ProtoT extends Message> extends BeamKafkaTable {
  private final transient Class<ProtoT> protoClass;

  public BeamKafkaProtoTable(
      Schema beamSchema, String bootstrapServers, List<String> topics, Class<ProtoT> protoClass) {
    super(beamSchema, bootstrapServers, topics);
    this.protoClass = protoClass;
  }

  @Override
  protected BeamKafkaTable getTable() {
    return this;
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    return new ProtoRecorderDecoder<>(schema, protoClass);
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> getPTransformForOutput() {
    return new ProtoRecorderEncoder<>(protoClass);
  }

  /** A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}. */
  private static class ProtoRecorderDecoder<ProtoT extends Message>
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {
    private final Schema schema;
    private final ProtoCoder<ProtoT> protoCoder;
    private final SerializableFunction<ProtoT, Row> toRowFunction;

    ProtoRecorderDecoder(Schema schema, Class<ProtoT> clazz) {
      this.schema = schema;
      this.protoCoder = ProtoCoder.of(clazz);
      this.toRowFunction = new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(clazz));
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      return input
          .apply(
              "decodeProtoRecord",
              ParDo.of(
                  new DoFn<KV<byte[], byte[]>, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Row decodedRow = decodeBytesToRow(c.element().getValue());
                      c.output(decodedRow);
                    }
                  }))
          .setRowSchema(schema);
    }

    private Row decodeBytesToRow(byte[] bytes) {
      try {
        InputStream inputStream = new ByteArrayInputStream(bytes);
        ProtoT message = protoCoder.decode(inputStream);
        return toRowFunction.apply(message);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not decode row from proto payload.", e);
      }
    }
  }

  /** A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}. */
  private static class ProtoRecorderEncoder<ProtoT extends Message>
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {
    private final SerializableFunction<Row, ProtoT> toProtoFunction;
    private final ProtoCoder<ProtoT> protoCoder;
    private final Class<ProtoT> clazz;

    public ProtoRecorderEncoder(Class<ProtoT> clazz) {
      this.protoCoder = ProtoCoder.of(clazz);
      this.toProtoFunction = new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(clazz));
      this.clazz = clazz;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      return input.apply(
          "encodeProtoRecord",
          ParDo.of(
              new DoFn<Row, KV<byte[], byte[]>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  Row row = c.element();
                  c.output(KV.of(new byte[] {}, encodeRowToProtoBytes(row)));
                }
              }));
    }

    byte[] encodeRowToProtoBytes(Row row) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try {
        Message message = toProtoFunction.apply(row);
        protoCoder.encode(clazz.cast(message), outputStream);
        return outputStream.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(String.format("Could not encode row %s to proto.", row), e);
      }
    }
  }
}
