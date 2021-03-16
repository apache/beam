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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.PartitionQuery.PartitionQueryResponseToRunQueryRequest;
import org.junit.Test;

public final class PartitionQueryResponseToRunQueryRequestTest {

  @Test
  public void ensureSortingCorrectlyHandlesPathSegments() {
    List<Cursor> expected =
        newArrayList(
            referenceValueCursor("projects/p1/databases/d1/documents/c1/doc1"),
            referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2"),
            referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2/c2/doc1"),
            referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2/c2/doc2"),
            referenceValueCursor("projects/p1/databases/d1/documents/c10/doc1"),
            referenceValueCursor("projects/p1/databases/d1/documents/c2/doc1"),
            referenceValueCursor("projects/p2/databases/d2/documents/c1/doc1"),
            referenceValueCursor("projects/p2/databases/d2/documents/c1-/doc1"),
            referenceValueCursor("projects/p2/databases/d3/documents/c1-/doc1"),
            referenceValueCursor("projects/p2/databases/d3/documents/c1-/doc1"),
            Cursor.newBuilder().build());

    for (int i = 0; i < 1000; i++) {
      List<Cursor> list = new ArrayList<>(expected);
      Collections.shuffle(list);

      list.sort(PartitionQueryResponseToRunQueryRequest.CURSOR_REFERENCE_VALUE_COMPARATOR);

      assertEquals(expected, list);
    }
  }

  private static Cursor referenceValueCursor(String referenceValue) {
    return Cursor.newBuilder()
        .addValues(Value.newBuilder().setReferenceValue(referenceValue).build())
        .build();
  }
}
