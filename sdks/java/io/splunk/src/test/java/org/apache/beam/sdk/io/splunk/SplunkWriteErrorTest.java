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
package org.apache.beam.sdk.io.splunk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/** Unit tests for {@link SplunkWriteError} class. */
public class SplunkWriteErrorTest {

  @Test
  public void testEquals() {

    String payload = "test-payload";
    String message = "test-message";
    Integer statusCode = 123;

    SplunkWriteError actualError =
        SplunkWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage(message)
            .create();

    assertEquals(
        SplunkWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage(message)
            .create(),
        actualError);

    assertNotEquals(
        SplunkWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage("a-different-message")
            .create(),
        actualError);
  }

  /** Test whether the default schema of {@link SplunkWriteError} has a field per accessor. */
  @Test
  public void testDefaultSchema() throws NoSuchSchemaException {
    Schema schema = SchemaRegistry.createDefault().getSchema(SplunkWriteError.class);

    assertEquals(
        Schema.builder()
            .addNullableField("statusCode", Schema.FieldType.INT32)
            .addNullableField("statusMessage", Schema.FieldType.STRING)
            .addNullableField("payload", Schema.FieldType.STRING)
            .build(),
        schema);
  }

  /** Test whether the default schema coder keeps the values of a {@link SplunkWriteError}. */
  @Test
  public void testSchemaCoderEncodeDecode() throws Exception {
    SchemaCoder<SplunkWriteError> coder =
        SchemaRegistry.createDefault().getSchemaCoder(SplunkWriteError.class);

    SplunkWriteError error =
        SplunkWriteError.newBuilder()
            .withPayload("test-payload")
            .withStatusCode(404)
            .withStatusMessage("test-message")
            .create();

    assertEquals(error, CoderUtils.clone(coder, error));
    CoderProperties.coderDecodeEncodeEqual(coder, error);
  }

  /** Test whether the default schema coder keeps a {@link SplunkWriteError} with missing values. */
  @Test
  public void testSchemaCoderEncodeDecodeNullValues() throws Exception {
    SchemaCoder<SplunkWriteError> coder =
        SchemaRegistry.createDefault().getSchemaCoder(SplunkWriteError.class);

    SplunkWriteError error = SplunkWriteError.newBuilder().withPayload("test-payload").create();

    assertEquals(error, CoderUtils.clone(coder, error));
  }

  /** Test whether a {@link SplunkWriteError} can be converted to a {@link Row} and back. */
  @Test
  public void testToRowAndFromRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    SplunkWriteError error =
        SplunkWriteError.newBuilder()
            .withPayload("test-payload")
            .withStatusCode(500)
            .withStatusMessage("test-message")
            .create();

    Row row = registry.getToRowFunction(SplunkWriteError.class).apply(error);

    assertEquals(Integer.valueOf(500), row.getInt32("statusCode"));
    assertEquals("test-message", row.getString("statusMessage"));
    assertEquals("test-payload", row.getString("payload"));
    assertEquals(error, registry.getFromRowFunction(SplunkWriteError.class).apply(row));
  }
}
