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
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamKafkaProtoTable extends BeamKafkaTable {
  private final Class<?> protoClass;

  public BeamKafkaProtoTable(
      Schema messageSchema, String bootstrapServers, List<String> topics, Class<?> protoClass) {
    super(inferAndVerifySchema(protoClass, messageSchema), bootstrapServers, topics);
    this.protoClass = protoClass;
  }

  @Override
  public PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    return new ProtoRecorderDecoder(schema, protoClass);
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> getPTransformForOutput() {
    return new ProtoRecorderEncoder(protoClass);
  }

  private static Schema inferAndVerifySchema(Class<?> protoClass, Schema messageSchema) {
    Schema inferredSchema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(protoClass));
    if (!messageSchema.equivalent(inferredSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "Given message schema: '%s'%n"
                  + "does not match schema inferred from protobuf class.%n"
                  + "Protobuf class: '%s'%n"
                  + "Inferred schema: '%s'",
              messageSchema, protoClass.getName(), inferredSchema));
    }
    return inferredSchema;
  }

  /** A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}. */
  private static class ProtoRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {
    private final Schema schema;
    private final Class<?> clazz;

    ProtoRecorderDecoder(Schema schema, Class<?> clazz) {
      this.schema = schema;
      this.clazz = clazz;
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      SimpleFunction<byte[], Row> toRowFn = ProtoMessageSchema.getProtoBytesToRowFn(clazz);
      return input
          .apply("decodeProtoRecord", MapElements.via(new KvToBytes()))
          .apply("Map bytes to rows", MapElements.via(toRowFn))
          .setRowSchema(schema);
    }

    private static class KvToBytes extends SimpleFunction<KV<byte[], byte[]>, byte[]> {
      @Override
      public byte[] apply(KV<byte[], byte[]> kv) {
        return kv.getValue();
      }
    }
  }

  /** A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}. */
  private static class ProtoRecorderEncoder
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {
    private final Class<?> clazz;

    public ProtoRecorderEncoder(Class<?> clazz) {
      this.clazz = clazz;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      SimpleFunction<Row, byte[]> toBytesFn = ProtoMessageSchema.getRowToProtoBytesFn(clazz);
      return input
          .apply("Encode proto bytes to row", MapElements.via(toBytesFn))
          .apply("Bytes to KV", MapElements.via(new BytesToKV()));
    }

    private static class BytesToKV extends SimpleFunction<byte[], KV<byte[], byte[]>> {
      @Override
      public KV<byte[], byte[]> apply(byte[] bytes) {
        return KV.of(new byte[] {}, bytes);
      }
    }
  }
}
