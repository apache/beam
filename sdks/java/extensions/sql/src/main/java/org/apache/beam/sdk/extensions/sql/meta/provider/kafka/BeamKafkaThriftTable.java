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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.thrift.ThriftCoder;
import org.apache.beam.sdk.io.thrift.ThriftSchema;
import org.apache.beam.sdk.schemas.RowMessages;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * Kafka table that handles thrift data.
 *
 * <p>Uses the default thrift schema {@link ThriftSchema#provider() provider} , so it can't handle
 * {@link ThriftSchema#custom() custom} container typedefs.
 *
 * @param <FieldT> thrift field type, i.e. {@code ThriftGeneratedClass._Fields}
 * @param <T> thrift generated class
 */
public class BeamKafkaThriftTable<FieldT extends TFieldIdEnum, T extends TBase<T, FieldT>>
    extends BeamKafkaTable {

  private static final SchemaProvider schemaProvider = ThriftSchema.provider();

  private final TypeDescriptor<T> typeDescriptor;
  private final Coder<T> coder;

  public BeamKafkaThriftTable(
      Schema requiredSchema,
      String bootstrapServers,
      List<String> topics,
      Class<T> thriftClass,
      TProtocolFactory protocolFactory) {
    super(thriftSchema(thriftClass, requiredSchema), bootstrapServers, topics);
    typeDescriptor = TypeDescriptor.of(thriftClass);
    coder = ThriftCoder.of(thriftClass, protocolFactory);
  }

  private static Schema thriftSchema(Class<?> thriftClass, Schema requiredSchema) {
    final TypeDescriptor<?> typeDescriptor = TypeDescriptor.of(thriftClass);
    final Schema schema =
        checkArgumentNotNull(BeamKafkaThriftTable.schemaProvider.schemaFor(typeDescriptor));
    if (!schema.assignableTo(requiredSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "Given message schema: '%s'%n"
                  + "does not match schema inferred from thrift class.%n"
                  + "Thrift class: '%s'%n"
                  + "Inferred schema: '%s'",
              requiredSchema, thriftClass.getName(), schema));
    }
    return schema;
  }

  @Override
  protected PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    return new InputTransformer(schema, typeDescriptor, coder);
  }

  private static class InputTransformer<T extends TBase<?, ?>>
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {

    private final Schema schema;
    private final SimpleFunction<byte[], Row> toRowFn;

    InputTransformer(Schema schema, TypeDescriptor<T> typeDescriptor, Coder<T> coder) {
      this.schema = schema;
      toRowFn = RowMessages.bytesToRowFn(schemaProvider, typeDescriptor, coder);
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      return input
          .apply("drop-kafka-keys", Values.create())
          .apply("thrift-to-row", MapElements.via(toRowFn))
          .setRowSchema(schema);
    }
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> getPTransformForOutput() {
    return new OutputTransformer(typeDescriptor, coder);
  }

  private static class OutputTransformer<T extends TBase<?, ?>>
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {

    private static final TypeDescriptor<byte[]> binTypeDescriptor = TypeDescriptor.of(byte[].class);
    private static final byte[] emptyKey = {};

    private final SimpleFunction<Row, byte[]> fromRowFn;

    OutputTransformer(TypeDescriptor<T> typeDescriptor, Coder<T> coder) {
      this.fromRowFn = RowMessages.rowToBytesFn(schemaProvider, typeDescriptor, coder);
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      return input
          .apply("row-to-thrift-bytes", MapElements.via(fromRowFn))
          .apply(
              "bytes-to-kvs",
              MapElements.into(TypeDescriptors.kvs(binTypeDescriptor, binTypeDescriptor))
                  .via(bytes -> KV.of(emptyKey, bytes)));
    }
  }
}
