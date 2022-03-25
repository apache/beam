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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySchemaTransformReadProvider.BigQuerySchemaTransformRead;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link BigQuerySchemaTransformReadProvider}. */
@RunWith(JUnit4.class)
public class BigQuerySchemaTransformReadProviderTest {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<BigQuerySchemaTransformReadConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(BigQuerySchemaTransformReadConfiguration.class);
  private static final Schema SCHEMA = AUTO_VALUE_SCHEMA.schemaFor(TYPE_DESCRIPTOR);
  private static final SerializableFunction<BigQuerySchemaTransformReadConfiguration, Row>
      ROW_SERIALIZABLE_FUNCTION = AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  @Test
  public void testFromExtractConfiguration() {
    BigQuerySchemaTransformReadConfiguration configuration =
        BigQuerySchemaTransformReadConfiguration.createExtractBuilder("dataset.table").build();
    Row configurationRow = ROW_SERIALIZABLE_FUNCTION.apply(configuration);
    SchemaTransformProvider provider = new BigQuerySchemaTransformReadProvider();
    BigQuerySchemaTransformRead transform =
        (BigQuerySchemaTransformRead) provider.from(configurationRow);
    assertEquals(configuration.getTableSpec(), transform.getConfiguration().getTableSpec());
  }

  @Test
  public void testFromQueryConfiguration() {
    BigQuerySchemaTransformReadConfiguration want =
        BigQuerySchemaTransformReadConfiguration.createQueryBuilder("select * from example")
            .build();
    Row configurationRow = ROW_SERIALIZABLE_FUNCTION.apply(want);
    SchemaTransformProvider provider = new BigQuerySchemaTransformReadProvider();
    BigQuerySchemaTransformRead transform =
        (BigQuerySchemaTransformRead) provider.from(configurationRow);
    BigQuerySchemaTransformReadConfiguration got = transform.getConfiguration();
    assertEquals(want.getQuery(), got.getQuery());
    assertEquals(want.getUseStandardSql(), got.getUseStandardSql());
  }

  @Test
  public void testGetConfiguration() {
    SchemaTransformProvider provider = new BigQuerySchemaTransformReadProvider();
    Schema got = provider.configurationSchema();
    assertEquals(SCHEMA, got);
  }
}
