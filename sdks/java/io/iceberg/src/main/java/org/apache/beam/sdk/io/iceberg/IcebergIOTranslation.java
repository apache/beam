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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows;
import static org.apache.beam.sdk.io.iceberg.IcebergIO.WriteRows;
import static org.apache.beam.sdk.util.construction.TransformUpgrader.fromByteArray;
import static org.apache.beam.sdk.util.construction.TransformUpgrader.toByteArray;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaAwareTransforms.SchemaAwareTransformPayload;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({"rawtypes", "nullness"})
public class IcebergIOTranslation {
  static class IcebergIOReadTranslator implements TransformPayloadTranslator<ReadRows> {

    static final Schema READ_SCHEMA =
        Schema.builder()
            .addByteArrayField("catalog_config")
            .addNullableStringField("table_identifier")
            .build();

    public static final String ICEBERG_READ_TRANSFORM_URN = "beam:transform:iceberg_read:v1";

    @Override
    public String getUrn() {
      return ICEBERG_READ_TRANSFORM_URN;
    }

    @Override
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, ReadRows> application, SdkComponents components)
        throws IOException {
      SchemaApi.Schema expansionSchema = SchemaTranslation.schemaToProto(READ_SCHEMA, true);
      Row configRow = toConfigRow(application.getTransform());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      RowCoder.of(READ_SCHEMA).encode(configRow, os);

      return FunctionSpec.newBuilder()
          .setUrn(getUrn())
          .setPayload(
              SchemaAwareTransformPayload.newBuilder()
                  .setExpansionSchema(expansionSchema)
                  .setExpansionPayload(ByteString.copyFrom(os.toByteArray()))
                  .build()
                  .toByteString())
          .build();
    }

    @Override
    public Row toConfigRow(ReadRows transform) {

      Map<String, Object> fieldValues = new HashMap<>();

      if (transform.getCatalogConfig() != null) {
        fieldValues.put(
            "catalog_config",
            toByteArray(new ExternalizableIcebergCatalogConfig(transform.getCatalogConfig())));
      }
      if (transform.getTableIdentifier() != null) {
        TableIdentifier identifier = transform.getTableIdentifier();
        List<String> identifierParts =
            Arrays.stream(identifier.namespace().levels()).collect(Collectors.toList());
        identifierParts.add(identifier.name());
        fieldValues.put("table_identifier", String.join(".", identifierParts));
      }

      return Row.withSchema(READ_SCHEMA).withFieldValues(fieldValues).build();
    }

    @Override
    public ReadRows fromConfigRow(Row configRow, PipelineOptions options) {
      try {
        ReadRows.Builder builder = new AutoValue_IcebergIO_ReadRows.Builder();

        byte[] catalogBytes = configRow.getBytes("catalog_config");
        if (catalogBytes != null) {
          builder =
              builder.setCatalogConfig(
                  ((ExternalizableIcebergCatalogConfig) fromByteArray(catalogBytes)).get());
        }
        String tableIdentifier = configRow.getString("table_identifier");
        if (tableIdentifier != null) {
          builder = builder.setTableIdentifier(TableIdentifier.parse(tableIdentifier));
        }
        return builder.build();
      } catch (InvalidClassException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ReadRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(AutoValue_IcebergIO_ReadRows.class, new IcebergIOReadTranslator())
          .build();
    }
  }

  static class IcebergIOWriteTranslator implements TransformPayloadTranslator<WriteRows> {

    static final Schema WRITE_SCHEMA =
        Schema.builder()
            .addByteArrayField("catalog_config")
            .addNullableStringField("table_identifier")
            .addNullableByteArrayField("dynamic_destinations")
            .build();

    public static final String ICEBERG_WRITE_TRANSFORM_URN = "beam:transform:iceberg_write:v1";

    @Override
    public String getUrn() {
      return ICEBERG_WRITE_TRANSFORM_URN;
    }

    @Override
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, WriteRows> application, SdkComponents components)
        throws IOException {
      SchemaApi.Schema expansionSchema = SchemaTranslation.schemaToProto(WRITE_SCHEMA, true);
      Row configRow = toConfigRow(application.getTransform());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      RowCoder.of(WRITE_SCHEMA).encode(configRow, os);

      return FunctionSpec.newBuilder()
          .setUrn(getUrn())
          .setPayload(
              SchemaAwareTransformPayload.newBuilder()
                  .setExpansionSchema(expansionSchema)
                  .setExpansionPayload(ByteString.copyFrom(os.toByteArray()))
                  .build()
                  .toByteString())
          .build();
    }

    @Override
    public Row toConfigRow(WriteRows transform) {

      Map<String, Object> fieldValues = new HashMap<>();

      if (transform.getCatalogConfig() != null) {
        fieldValues.put(
            "catalog_config",
            toByteArray(new ExternalizableIcebergCatalogConfig(transform.getCatalogConfig())));
      }
      if (transform.getTableIdentifier() != null) {
        TableIdentifier identifier = transform.getTableIdentifier();
        List<String> identifierParts =
            Arrays.stream(identifier.namespace().levels()).collect(Collectors.toList());
        identifierParts.add(identifier.name());
        fieldValues.put("table_identifier", String.join(".", identifierParts));
      }
      if (transform.getDynamicDestinations() != null) {
        fieldValues.put("dynamic_destinations", toByteArray(transform.getDynamicDestinations()));
      }

      return Row.withSchema(WRITE_SCHEMA).withFieldValues(fieldValues).build();
    }

    @Override
    public WriteRows fromConfigRow(Row configRow, PipelineOptions options) {
      try {
        IcebergIO.WriteRows.Builder builder = new AutoValue_IcebergIO_WriteRows.Builder();

        byte[] catalogBytes = configRow.getBytes("catalog_config");
        if (catalogBytes != null) {
          builder =
              builder.setCatalogConfig(
                  ((ExternalizableIcebergCatalogConfig) fromByteArray(catalogBytes)).get());
        }
        String tableIdentifier = configRow.getString("table_identifier");
        if (tableIdentifier != null) {
          builder = builder.setTableIdentifier(TableIdentifier.parse(tableIdentifier));
        }
        byte[] dynamicDestinationsBytes = configRow.getBytes("dynamic_destinations");
        if (dynamicDestinationsBytes != null) {
          builder =
              builder.setDynamicDestinations(
                  (DynamicDestinations) fromByteArray(dynamicDestinationsBytes));
        }
        return builder.build();
      } catch (InvalidClassException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class WriteRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(AutoValue_IcebergIO_WriteRows.class, new IcebergIOWriteTranslator())
          .build();
    }
  }
}
