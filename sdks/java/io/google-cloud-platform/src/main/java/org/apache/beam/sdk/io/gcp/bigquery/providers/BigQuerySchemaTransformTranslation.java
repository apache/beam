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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransform;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryFileLoadsWriteSchemaTransformProvider.BigQueryFileLoadsSchemaTransform;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransform;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class BigQuerySchemaTransformTranslation {
  public static class BigQueryStorageReadSchemaTransformTranslator
      extends SchemaTransformTranslation.SchemaTransformPayloadTranslator<
          BigQueryDirectReadSchemaTransform> {
    @Override
    public SchemaTransformProvider provider() {
      return new BigQueryDirectReadSchemaTransformProvider();
    }

    @Override
    public Row toConfigRow(BigQueryDirectReadSchemaTransform transform) {
      return transform.getConfigurationRow();
    }
  }

  public static class BigQueryStorageWriteSchemaTransformTranslator
      extends SchemaTransformTranslation.SchemaTransformPayloadTranslator<
          BigQueryStorageWriteApiSchemaTransform> {
    @Override
    public SchemaTransformProvider provider() {
      return new BigQueryStorageWriteApiSchemaTransformProvider();
    }

    @Override
    public Row toConfigRow(BigQueryStorageWriteApiSchemaTransform transform) {
      return transform.getConfigurationRow();
    }
  }

  public static class BigQueryFileLoadsSchemaTransformTranslator
      extends SchemaTransformTranslation.SchemaTransformPayloadTranslator<
          BigQueryFileLoadsSchemaTransform> {
    @Override
    public SchemaTransformProvider provider() {
      return new BigQueryFileLoadsWriteSchemaTransformProvider();
    }

    @Override
    public Row toConfigRow(BigQueryFileLoadsSchemaTransform transform) {
      return transform.getConfigurationRow();
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ReadWriteRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              BigQueryDirectReadSchemaTransform.class,
              new BigQueryStorageReadSchemaTransformTranslator())
          .put(
              BigQueryStorageWriteApiSchemaTransform.class,
              new BigQueryStorageWriteSchemaTransformTranslator())
          .put(
              BigQueryFileLoadsSchemaTransform.class,
              new BigQueryFileLoadsSchemaTransformTranslator())
          .build();
    }
  }
}
