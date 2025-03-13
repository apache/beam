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

import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.IcebergReadSchemaTransform;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.IcebergWriteSchemaTransform;
import static org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslation.SchemaTransformPayloadTranslator;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings({"rawtypes", "nullness"})
public class IcebergSchemaTransformTranslation {
  static class IcebergReadSchemaTransformTranslator
      extends SchemaTransformPayloadTranslator<IcebergReadSchemaTransform> {
    @Override
    public SchemaTransformProvider provider() {
      return new IcebergReadSchemaTransformProvider();
    }

    @Override
    public Row toConfigRow(IcebergReadSchemaTransform transform) {
      return transform.getConfigurationRow();
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
          .put(IcebergReadSchemaTransform.class, new IcebergReadSchemaTransformTranslator())
          .build();
    }
  }

  static class IcebergWriteSchemaTransformTranslator
      extends SchemaTransformPayloadTranslator<IcebergWriteSchemaTransform> {
    @Override
    public SchemaTransformProvider provider() {
      return new IcebergWriteSchemaTransformProvider();
    }

    @Override
    public Row toConfigRow(IcebergWriteSchemaTransform transform) {
      return transform.getConfigurationRow();
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
          .put(IcebergWriteSchemaTransform.class, new IcebergWriteSchemaTransformTranslator())
          .build();
    }
  }
}
