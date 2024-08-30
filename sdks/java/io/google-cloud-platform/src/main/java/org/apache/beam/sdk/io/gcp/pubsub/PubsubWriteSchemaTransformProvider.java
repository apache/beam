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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
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

  public static final String VALID_FORMATS_STR = "RAW,AVRO,JSON";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  @Override
  public Class<PubsubWriteSchemaTransformConfiguration> configurationClass() {
    return PubsubWriteSchemaTransformConfiguration.class;
  }

  public static class ErrorFn extends DoFn<Row, PubsubMessage> {
    private final SerializableFunction<Row, byte[]> valueMapper;
    private final @Nullable Set<String> attributes;
    private final @Nullable String attributesMap;
    private final Schema payloadSchema;
    private final Schema errorSchema;
    private final boolean useErrorOutput;

    ErrorFn(
        SerializableFunction<Row, byte[]> valueMapper,
        @Nullable List<String> attributes,
        @Nullable String attributesMap,
        Schema payloadSchema,
        Schema errorSchema,
        boolean useErrorOutput) {
      this.valueMapper = valueMapper;
      this.attributes = attributes == null ? null : ImmutableSet.copyOf(attributes);
      this.attributesMap = attributesMap;
      this.payloadSchema = payloadSchema;
      this.errorSchema = errorSchema;
      this.useErrorOutput = useErrorOutput;
    }

    @ProcessElement
    public void processElement(@Element Row row, MultiOutputReceiver receiver) throws Exception {
      try {
        Row payloadRow;
        Map<String, String> messageAttributes = null;
        if (attributes == null && attributesMap == null) {
          payloadRow = row;
        } else {
          Row.Builder payloadRowBuilder = Row.withSchema(payloadSchema);
          messageAttributes = new HashMap<>();
          List<Schema.Field> fields = row.getSchema().getFields();
          for (int ix = 0; ix < fields.size(); ix++) {
            String name = fields.get(ix).getName();
            if (attributes != null && attributes.contains(name)) {
              messageAttributes.put(name, row.getValue(ix));
            } else if (name.equals(attributesMap)) {
              Map<String, String> attrs = row.<String, String>getMap(ix);
              if (attrs != null) {
                messageAttributes.putAll(attrs);
              }
            } else {
              payloadRowBuilder.addValue(row.getValue(ix));
            }
          }
          payloadRow = payloadRowBuilder.build();
        }
        receiver
            .get(OUTPUT_TAG)
            .output(new PubsubMessage(valueMapper.apply(payloadRow), messageAttributes));
      } catch (Exception e) {
        if (useErrorOutput) {
          receiver
              .get(ERROR_TAG)
              .output(Row.withSchema(errorSchema).addValues(e.toString(), row).build());
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public SchemaTransform from(PubsubWriteSchemaTransformConfiguration configuration) {
    if (!VALID_DATA_FORMATS.contains(configuration.getFormat().toUpperCase())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
    return new PubsubWriteSchemaTransform(configuration);
  }

  private static class PubsubWriteSchemaTransform extends SchemaTransform implements Serializable {
    final PubsubWriteSchemaTransformConfiguration configuration;

    PubsubWriteSchemaTransform(PubsubWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    @SuppressWarnings({
      "nullness" // TODO(https://github.com/apache/beam/issues/20497)
    })
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      String errorOutput =
          configuration.getErrorHandling() == null
              ? null
              : configuration.getErrorHandling().getOutput();

      final Schema errorSchema =
          Schema.builder()
              .addStringField("error")
              .addNullableRowField("row", input.get("input").getSchema())
              .build();

      String format = configuration.getFormat();
      Schema beamSchema = input.get("input").getSchema();
      Schema payloadSchema;
      if (configuration.getAttributes() == null && configuration.getAttributesMap() == null) {
        payloadSchema = beamSchema;
      } else {
        Schema.Builder payloadSchemaBuilder = Schema.builder();
        for (Schema.Field f : beamSchema.getFields()) {
          if (!configuration.getAttributes().contains(f.getName())
              && !f.getName().equals(configuration.getAttributesMap())) {
            payloadSchemaBuilder.addField(f);
          }
        }
        payloadSchema = payloadSchemaBuilder.build();
      }
      SerializableFunction<Row, byte[]> fn;
      if (Objects.equals(format, "RAW")) {
        if (payloadSchema.getFieldCount() != 1) {
          throw new IllegalArgumentException(
              String.format(
                  "Raw output only supported for single-field schemas, got %s", payloadSchema));
        }
        if (payloadSchema.getField(0).getType().equals(Schema.FieldType.BYTES)) {
          fn = row -> row.getBytes(0);
        } else if (payloadSchema.getField(0).getType().equals(Schema.FieldType.STRING)) {
          fn = row -> row.getString(0).getBytes(StandardCharsets.UTF_8);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Raw output only supports bytes and string fields, got %s",
                  payloadSchema.getField(0)));
        }
      } else if (Objects.equals(format, "JSON")) {
        fn = JsonUtils.getRowToJsonBytesFunction(payloadSchema);
      } else if (Objects.equals(format, "AVRO")) {
        fn = AvroUtils.getRowToAvroBytesFunction(payloadSchema);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Format %s not supported. Only supported formats are %s",
                format, VALID_FORMATS_STR));
      }

      PCollectionTuple outputTuple =
          input
              .get("input")
              .apply(
                  ParDo.of(
                          new ErrorFn(
                              fn,
                              configuration.getAttributes(),
                              configuration.getAttributesMap(),
                              payloadSchema,
                              errorSchema,
                              errorOutput != null))
                      .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PubsubIO.Write<PubsubMessage> writeTransform =
          PubsubIO.writeMessages().to(configuration.getTopic());
      if (!Strings.isNullOrEmpty(configuration.getIdAttribute())) {
        writeTransform = writeTransform.withIdAttribute(configuration.getIdAttribute());
      }
      if (!Strings.isNullOrEmpty(configuration.getTimestampAttribute())) {
        writeTransform = writeTransform.withIdAttribute(configuration.getTimestampAttribute());
      }
      outputTuple.get(OUTPUT_TAG).apply(writeTransform);
      outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      if (errorOutput == null) {
        return PCollectionRowTuple.empty(input.getPipeline());
      } else {
        return PCollectionRowTuple.of(
            errorOutput, outputTuple.get(ERROR_TAG).setRowSchema(errorSchema));
      }
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
