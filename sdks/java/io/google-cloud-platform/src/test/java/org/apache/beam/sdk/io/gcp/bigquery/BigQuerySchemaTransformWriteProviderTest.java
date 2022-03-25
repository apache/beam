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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.*;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformWriteProvider.BigQuerySchemaTransformWrite;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;

public class BigQuerySchemaTransformWriteProviderTest {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigQuerySchemaTransformWriteConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigQuerySchemaTransformWriteConfiguration.class);
  private static final Schema SCHEMA = AUTO_VALUE_SCHEMA.schemaFor(TYPE_DESCRIPTOR);
  private static final SerializableFunction<BigQuerySchemaTransformWriteConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  @Test
  public void testFrom() {
    BigQuerySchemaTransformWriteConfiguration configuration =
        BigQuerySchemaTransformWriteConfiguration.createLoad(
            "dataset.table", CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND);
    Row configurationRow = ROW_SERIALIZABLE_FUNCTION.apply(configuration);
    SchemaTransformProvider provider = new BigQuerySchemaTransformWriteProvider();
    BigQuerySchemaTransformWrite transform =
        (BigQuerySchemaTransformWrite) provider.from(configurationRow);
    assertEquals(configuration.getTableSpec(), transform.getConfiguration().getTableSpec());
  }

  @Test
  public void testGetConfiguration() {
    SchemaTransformProvider provider = new BigQuerySchemaTransformWriteProvider();
    Schema got = provider.configurationSchema();
    assertEquals(SCHEMA, got);
  }
}
