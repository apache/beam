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

import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQuerySchemaIOProviderTest {

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final Schema SCHEMA = AUTO_VALUE_SCHEMA.schemaFor(
      TypeDescriptor.of(BigQuerySchemaIOConfiguration.class));
  private static final SerializableFunction<BigQuerySchemaIOConfiguration, Row> ROW_SERIALIZABLE_FUNCTION =
      AUTO_VALUE_SCHEMA.toRowFunction(TypeDescriptor.of(BigQuerySchemaIOConfiguration.class));

  @Test
  public void testConfigurationSchema() {
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Schema got = provider.configurationSchema();
    assertEquals(SCHEMA, got);
  }

  @Test
  public void testFrom() {
    String query = "select * from example";
    BigQuerySchemaIOConfiguration want = BigQuerySchemaIOConfiguration.builderOfQueryType(query)
        .build();
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Row inputConfig = ROW_SERIALIZABLE_FUNCTION.apply(want);
    SchemaTransform schemaTransform = provider.from(inputConfig);
    BigQuerySchemaTransform bigQuerySchemaTransform = (BigQuerySchemaTransform) schemaTransform;
    BigQuerySchemaIOConfiguration got = bigQuerySchemaTransform.getConfiguration();
    assertEquals(want, got);
    assertEquals(query, got.getQuery());
  }
}