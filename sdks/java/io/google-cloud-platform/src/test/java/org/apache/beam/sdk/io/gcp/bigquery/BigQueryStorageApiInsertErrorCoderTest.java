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
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link BigQueryStorageApiInsertErrorCoder}. */
@RunWith(JUnit4.class)
public class BigQueryStorageApiInsertErrorCoderTest {

  private static final Coder<BigQueryStorageApiInsertError> TEST_CODER =
      BigQueryStorageApiInsertErrorCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    TableRow row = new TableRow().set("field1", "value1").set("field2", 123);
    BigQueryStorageApiInsertError value =
        new BigQueryStorageApiInsertError(
            row,
            "An error message",
            new TableReference()
                .setProjectId("dummy-project-id")
                .setDatasetId("dummy-dataset-id")
                .setTableId("dummy-table-id"));

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    TEST_CODER.encode(value, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    BigQueryStorageApiInsertError decoded = TEST_CODER.decode(inStream);

    assertEquals(value.getRow(), decoded.getRow());
    assertEquals(value.getErrorMessage(), decoded.getErrorMessage());
    assertEquals("dummy-project-id", decoded.getTable().getProjectId());
    assertEquals("dummy-dataset-id", decoded.getTable().getDatasetId());
    assertEquals("dummy-table-id", decoded.getTable().getTableId());
  }

  @Test
  public void testDecodeEncodeWithNullTable() throws Exception {
    TableRow row = new TableRow().set("field1", "value1");
    BigQueryStorageApiInsertError value =
        new BigQueryStorageApiInsertError(row, "An error message", null);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    TEST_CODER.encode(value, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    BigQueryStorageApiInsertError decoded = TEST_CODER.decode(inStream);

    assertEquals(value.getRow(), decoded.getRow());
    assertEquals(value.getErrorMessage(), decoded.getErrorMessage());
    assertNull(decoded.getTable());
  }

  @Test
  public void testDecodeEncodeWithNullErrorMessage() throws Exception {
    TableRow row = new TableRow().set("field1", "value1");
    BigQueryStorageApiInsertError value =
        new BigQueryStorageApiInsertError(
            row,
            null,
            new TableReference()
                .setProjectId("dummy-project-id")
                .setDatasetId("dummy-dataset-id")
                .setTableId("dummy-table-id"));

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    TEST_CODER.encode(value, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    BigQueryStorageApiInsertError decoded = TEST_CODER.decode(inStream);

    assertEquals(value.getRow(), decoded.getRow());
    assertNull(decoded.getErrorMessage());
    assertEquals("dummy-project-id", decoded.getTable().getProjectId());
    assertEquals("dummy-dataset-id", decoded.getTable().getDatasetId());
    assertEquals("dummy-table-id", decoded.getTable().getTableId());
  }

  @Test
  public void testDecodeEncodeWithAllNullableFieldsNull() throws Exception {
    TableRow row = new TableRow().set("field1", "value1");
    BigQueryStorageApiInsertError value = new BigQueryStorageApiInsertError(row, null, null);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    TEST_CODER.encode(value, outStream);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    BigQueryStorageApiInsertError decoded = TEST_CODER.decode(inStream);

    assertEquals(value.getRow(), decoded.getRow());
    assertNull(decoded.getErrorMessage());
    assertNull(decoded.getTable());
  }
}
