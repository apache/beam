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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Pub/Sub reads configured using
 * {@link PubsubWriteSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class PubsubWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<PubsubWriteSchemaTransformConfiguration> {

  public static final TupleTag<PubsubMessage> OUTPUT_TAG = new TupleTag<PubsubMessage>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  public static final String VALID_FORMATS_STR = "AVRO,JSON";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  @Override
  public Class<PubsubWriteSchemaTransformConfiguration> configurationClass() {
    return PubsubWriteSchemaTransformConfiguration.class;
  }

  public static class ErrorFn extends DoFn<Row, PubsubMessage> {
    private SerializableFunction<Row, byte[]> valueMapper;
    private Schema errorSchema;

    ErrorFn(SerializableFunction<Row, byte[]> valueMapper, Schema errorSchema) {
      this.valueMapper = valueMapper;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(@Element Row row, MultiOutputReceiver receiver) {
      try {
        receiver.get(OUTPUT_TAG).output(new PubsubMessage(valueMapper.apply(row), null));
      } catch (Exception e) {
        receiver
            .get(ERROR_TAG)
            .output(Row.withSchema(errorSchema).addValues(e.toString(), row).build());
      }
    }
  }

  @Override
  public SchemaTransform from(PubsubWriteSchemaTransformConfiguration configuration) {
    if (!VALID_DATA_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
    return new PubsubWriteSchemaTransform(configuration.getTopic(), configuration.getFormat());
  }

  private static class PubsubWriteSchemaTransform extends SchemaTransform implements Serializable {
    final String topic;
    final String format;

    PubsubWriteSchemaTransform(String topic, String format) {
      this.topic = topic;
      this.format = format;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      final Schema errorSchema =
          Schema.builder()
              .addStringField("error")
              .addNullableRowField("row", input.get("input").getSchema())
              .build();
      SerializableFunction<Row, byte[]> fn =
          format.equals("AVRO")
              ? AvroUtils.getRowToAvroBytesFunction(input.get("input").getSchema())
              : JsonUtils.getRowToJsonBytesFunction(input.get("input").getSchema());

      PCollectionTuple outputTuple =
          input
              .get("input")
              .apply(
                  ParDo.of(new ErrorFn(fn, errorSchema))
                      .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      outputTuple.get(OUTPUT_TAG).apply(PubsubIO.writeMessages().to(topic));

      return PCollectionRowTuple.of("errors", outputTuple.get(ERROR_TAG).setRowSchema(errorSchema));
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:pubsub_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("errors");
  }
}
