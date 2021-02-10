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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class BeamKafkaAvroTable extends BeamKafkaTable {

  public BeamKafkaAvroTable(Schema beamSchema, String bootstrapServers, List<String> topics) {
    super(beamSchema, bootstrapServers, topics);
  }

  @Override
  protected PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    return new AvroRecorderDecoder(schema);
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> getPTransformForOutput() {
    return new AvroRecorderEncoder(schema);
  }

  /** A PTransform to convert {@code KV<byte[], byte[]>} to {@link Row}. */
  private static class AvroRecorderDecoder
      extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<Row>> {
    private final Schema schema;

    AvroRecorderDecoder(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<KV<byte[], byte[]>> input) {
      return input
          .apply(
              "extractValue", MapElements.into(TypeDescriptor.of(byte[].class)).via(KV::getValue))
          .apply("decodeAvroRecord", MapElements.via(AvroUtils.getAvroBytesToRowFunction(schema)))
          .setRowSchema(schema);
    }
  }

  /** A PTransform to convert {@link Row} to {@code KV<byte[], byte[]>}. */
  private static class AvroRecorderEncoder
      extends PTransform<PCollection<Row>, PCollection<KV<byte[], byte[]>>> {
    private final Schema schema;

    AvroRecorderEncoder(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollection<KV<byte[], byte[]>> expand(PCollection<Row> input) {
      return input
          .apply("encodeAvroRecord", MapElements.via(AvroUtils.getRowToAvroBytesFunction(schema)))
          .apply("mapToKV", MapElements.via(new MakeBytesKVFn()));
    }

    private static class MakeBytesKVFn extends SimpleFunction<byte[], KV<byte[], byte[]>> {
      @Override
      public KV<byte[], byte[]> apply(byte[] bytes) {
        return KV.of(new byte[] {}, bytes);
      }
    }
  }
}
