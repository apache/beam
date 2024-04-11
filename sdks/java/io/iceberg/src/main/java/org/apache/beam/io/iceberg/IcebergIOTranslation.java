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
package org.apache.beam.io.iceberg;

import static org.apache.beam.io.iceberg.IcebergIO.WriteRows;
import static org.apache.beam.sdk.util.construction.TransformUpgrader.fromByteArray;
import static org.apache.beam.sdk.util.construction.TransformUpgrader.toByteArray;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
  static class IcebergIOWriteTranslator implements TransformPayloadTranslator<WriteRows> {

    static Schema schema =
        Schema.builder()
            .addByteArrayField("catalog_config")
            .addNullableArrayField("table_identifier", FieldType.STRING)
            .addNullableByteArrayField("dynamic_destinations")
            .build();

    public static final String ICEBERG_WRITE_TRANSFORM_URN =
        "beam:transform:org.apache.beam:iceberg_write:v1";

    @Override
    public String getUrn() {
      return ICEBERG_WRITE_TRANSFORM_URN;
    }

    @Override
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, WriteRows> application, SdkComponents components)
        throws IOException {
      // Setting an empty payload since Iceberg transform payload is not actually used by runners
      // currently.
      return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
    }

    @Override
    public Row toConfigRow(WriteRows transform) {

      Map<String, Object> fieldValues = new HashMap<>();

      if (transform.getCatalogConfig() != null) {
        fieldValues.put("catalog_config", toByteArray(transform.getCatalogConfig()));
      }
      if (transform.getTableIdentifier() != null) {
        TableIdentifier identifier = transform.getTableIdentifier();
        List<String> identifierParts =
            Arrays.stream(identifier.namespace().levels()).collect(Collectors.toList());
        identifierParts.add(identifier.name());
        fieldValues.put("table_identifier", identifierParts);
      }
      if (transform.getDynamicDestinations() != null) {
        fieldValues.put("dynamic_destinations", toByteArray(transform.getDynamicDestinations()));
      }

      return Row.withSchema(schema).withFieldValues(fieldValues).build();
    }

    @Override
    public WriteRows fromConfigRow(Row configRow, PipelineOptions options) {
      try {
        IcebergIO.WriteRows.Builder builder = new AutoValue_IcebergIO_WriteRows.Builder();

        byte[] catalogBytes = configRow.getBytes("catalog_config");
        if (catalogBytes != null) {
          builder = builder.setCatalogConfig((IcebergCatalogConfig) fromByteArray(catalogBytes));
        }
        Collection<String> tableIdentifierParts = configRow.getArray("table_identifier");
        if (tableIdentifierParts != null) {
          builder =
              builder.setTableIdentifier(
                  TableIdentifier.parse(String.join(".", tableIdentifierParts)));
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
