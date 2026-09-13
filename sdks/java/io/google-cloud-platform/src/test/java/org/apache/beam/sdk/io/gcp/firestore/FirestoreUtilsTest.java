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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.junit.Assert.assertEquals;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FirestoreUtilsTest {

  @Test
  public void testDocumentRowRoundTrip() {
    Schema schema = Schema.builder().addStringField("document_id").addStringField("name").build();
    Row input = Row.withSchema(schema).addValues("doc-1", "Alice").build();
    Document document =
        FirestoreUtils.rowToDocument(
            input, schema, "test-project", "(default)", "users", "document_id");
    Row output = FirestoreUtils.documentToRow(document, schema, "document_id");

    assertEquals("doc-1", output.getString("document_id"));
    assertEquals("Alice", output.getString("name"));
    assertEquals("Alice", document.getFieldsMap().get("name").getStringValue());
  }

  @Test
  public void testDocumentIdFromName() {
    assertEquals(
        "doc-1",
        FirestoreUtils.documentIdFromName("projects/p/databases/(default)/documents/users/doc-1"));
  }

  @Test
  public void testIntegerValueConversion() {
    Schema schema = Schema.builder().addInt64Field("count").build();
    Document document =
        Document.newBuilder()
            .setName("projects/p/databases/(default)/documents/users/doc-1")
            .putFields("count", Value.newBuilder().setIntegerValue(42L).build())
            .build();
    Row row = FirestoreUtils.documentToRow(document, schema, null);
    assertEquals(42L, row.getInt64("count").longValue());
  }
}
