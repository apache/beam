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
package org.apache.beam.sdk.io.gcp.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.gcp.datastore.DataStoreV1SchemaIOProvider.DataStoreV1SchemaIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataStoreV1SchemaIOProviderTest {
  static final String DEFAULT_KEY_FIELD = "__key__";
  public static final String KEY_FIELD_PROPERTY = "keyField";
  private DataStoreV1SchemaIOProvider provider = new DataStoreV1SchemaIOProvider();

  @Test
  public void testGetTableType() {
    assertEquals("datastoreV1", provider.identifier());
  }

  @Test
  public void testBuildBeamSqlTable() {
    final String location = "projectId/batch_kind";

    Row configuration =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue(KEY_FIELD_PROPERTY, null)
            .build();
    SchemaIO schemaIO = provider.from(location, configuration, generateDataSchema());

    assertNotNull(schemaIO);
    assertTrue(schemaIO instanceof DataStoreV1SchemaIO);

    DataStoreV1SchemaIO dataStoreV1SchemaIO = (DataStoreV1SchemaIO) schemaIO;
    assertEquals("projectId", dataStoreV1SchemaIO.projectId);
    assertEquals("batch_kind", dataStoreV1SchemaIO.kind);
    assertEquals(DEFAULT_KEY_FIELD, dataStoreV1SchemaIO.keyField);
  }

  @Test
  public void testTableProperty() {
    final String location = "projectId/batch_kind";

    Row configuration =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue(KEY_FIELD_PROPERTY, "field_name")
            .build();
    SchemaIO schemaIO = provider.from(location, configuration, generateDataSchema());

    assertNotNull(schemaIO);
    assertTrue(schemaIO instanceof DataStoreV1SchemaIO);

    DataStoreV1SchemaIO dataStoreV1SchemaIO = (DataStoreV1SchemaIO) schemaIO;
    assertEquals("projectId", dataStoreV1SchemaIO.projectId);
    assertEquals("batch_kind", dataStoreV1SchemaIO.kind);
    assertEquals("field_name", dataStoreV1SchemaIO.keyField);
  }

  @Test
  public void testTableProperty_nullValue_throwsException() {
    final String location = "projectId/batch_kind";

    Row configuration =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue(KEY_FIELD_PROPERTY, "")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            (new DataStoreV1SchemaIOProvider())
                .from(location, configuration, generateDataSchema()));
  }

  private static Schema generateDataSchema() {
    return Schema.builder()
        .addNullableField("id", Schema.FieldType.INT32)
        .addNullableField("name", Schema.FieldType.STRING)
        .build();
  }
}
