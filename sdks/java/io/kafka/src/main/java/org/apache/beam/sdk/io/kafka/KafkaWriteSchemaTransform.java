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
package org.apache.beam.sdk.io.kafka;

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes"})
public class KafkaWriteSchemaTransform implements SchemaTransform {

  private final KafkaWriteSchemaTransformConfiguration configuration;

  private static final SchemaRegistry SCHEMA_REGISTRY = SchemaRegistry.createDefault();

  KafkaWriteSchemaTransform(KafkaWriteSchemaTransformConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
    return new PCollectionRowTupleTransform(this.configuration);
  }

  static class RowToKVFn extends SimpleFunction<Row, KV<byte[], byte[]>> {
    private SerializableFunction<Row, KVConfig> fromRowFunc;

    @Override
    public KV<byte[], byte[]> apply(Row input) {
      if (fromRowFunc == null) {
        Schema kvConfigSchema;
        try {
          kvConfigSchema = SCHEMA_REGISTRY.getSchema(KVConfig.class);
        } catch (NoSuchSchemaException e) {
          throw new RuntimeException(e);
        }

        if (!input.getSchema().assignableTo(kvConfigSchema)) {
          throw new RuntimeException(
              String.format(
                  "Expected rows with schema %s but received rows with schema %s",
                  kvConfigSchema, input.getSchema()));
        }

        try {
          fromRowFunc = SCHEMA_REGISTRY.getFromRowFunction(KVConfig.class);
        } catch (NoSuchSchemaException e) {
          throw new RuntimeException(e);
        }
      }

      KVConfig config = fromRowFunc.apply(input);
      return KV.of(config.key, config.value);
    }
  }

  static class PCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final KafkaWriteSchemaTransformConfiguration configuration;

    PCollectionRowTupleTransform(KafkaWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    private static Class resolveClass(String className) {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class: " + className);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rowPCollection = input.get(KafkaWriteSchemaTransformProvider.INPUT_TAG);

      Class keySerializer = resolveClass(this.configuration.keySerializer);
      Class valSerializer = resolveClass(this.configuration.valueSerializer);

      rowPCollection
          .apply(MapElements.via(new RowToKVFn()))
          .apply(
              KafkaIO.writeRecords()
                  .withBootstrapServers(this.configuration.bootstrapServers)
                  .withTopic(this.configuration.topic)
                  .withKeySerializer(keySerializer)
                  .withValueSerializer(valSerializer));

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }
}
