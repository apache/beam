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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.PartitionQuery.PartitionQueryResponseToRunQueryRequest;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryPair;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class PartitionQueryResponseToRunQueryRequestTest {

  @Mock protected DoFn<PartitionQueryPair, RunQueryRequest>.ProcessContext processContext;

  private final StructuredQuery query =
      StructuredQuery.newBuilder()
          .addFrom(
              CollectionSelector.newBuilder().setAllDescendants(true).setCollectionId("c1").build())
          .build();

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

  @Test
  public void ensureCursorPairingWorks() {
    Cursor cursor1 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc1");
    Cursor cursor2 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2");
    Cursor cursor3 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2/c2/doc2");

    List<StructuredQuery> expectedQueries =
        newArrayList(
            newQueryWithCursors(query, null, cursor1),
            newQueryWithCursors(query, cursor1, cursor2),
            newQueryWithCursors(query, cursor2, cursor3),
            newQueryWithCursors(query, cursor3, null));

    PartitionQueryPair partitionQueryPair =
        new PartitionQueryPair(
            PartitionQueryRequest.newBuilder().setStructuredQuery(query).build(),
            PartitionQueryResponse.newBuilder()
                .addPartitions(cursor3)
                .addPartitions(cursor1)
                .addPartitions(cursor2)
                .build());

    ArgumentCaptor<RunQueryRequest> captor = ArgumentCaptor.forClass(RunQueryRequest.class);
    when(processContext.element()).thenReturn(partitionQueryPair);
    doNothing().when(processContext).output(captor.capture());

    PartitionQueryResponseToRunQueryRequest fn = new PartitionQueryResponseToRunQueryRequest();
    fn.processElement(processContext);

    List<StructuredQuery> actualQueries =
        captor.getAllValues().stream()
            .map(RunQueryRequest::getStructuredQuery)
            .collect(Collectors.toList());

    assertEquals(expectedQueries, actualQueries);
  }

  @Test
  public void ensureCursorPairingWorks_emptyCursorsInResponse() {
    List<StructuredQuery> expectedQueries = newArrayList(query);

    PartitionQueryPair partitionQueryPair =
        new PartitionQueryPair(
            PartitionQueryRequest.newBuilder().setStructuredQuery(query).build(),
            PartitionQueryResponse.newBuilder().build());

    ArgumentCaptor<RunQueryRequest> captor = ArgumentCaptor.forClass(RunQueryRequest.class);
    when(processContext.element()).thenReturn(partitionQueryPair);
    doNothing().when(processContext).output(captor.capture());

    PartitionQueryResponseToRunQueryRequest fn = new PartitionQueryResponseToRunQueryRequest();
    fn.processElement(processContext);

    List<StructuredQuery> actualQueries =
        captor.getAllValues().stream()
            .map(RunQueryRequest::getStructuredQuery)
            .collect(Collectors.toList());

    assertEquals(expectedQueries, actualQueries);
  }

  @Test
  public void withoutReadTime() {
    Cursor cursor1 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc1");
    Cursor cursor2 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2");
    Cursor cursor3 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2/c2/doc2");

    PartitionQueryPair partitionQueryPair =
        new PartitionQueryPair(
            PartitionQueryRequest.newBuilder().setStructuredQuery(query).build(),
            PartitionQueryResponse.newBuilder()
                .addPartitions(cursor3)
                .addPartitions(cursor1)
                .addPartitions(cursor2)
                .build());

    ArgumentCaptor<RunQueryRequest> captor = ArgumentCaptor.forClass(RunQueryRequest.class);
    when(processContext.element()).thenReturn(partitionQueryPair);
    doNothing().when(processContext).output(captor.capture());

    PartitionQueryResponseToRunQueryRequest fn = new PartitionQueryResponseToRunQueryRequest();
    fn.processElement(processContext);

    List<RunQueryRequest> result = captor.getAllValues();
    assertEquals(4, result.size());
    result.forEach(r -> assertEquals(Timestamp.getDefaultInstance(), r.getReadTime()));
  }

  @Test
  public void withReadTime() {
    Instant now = Instant.now();

    Cursor cursor1 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc1");
    Cursor cursor2 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2");
    Cursor cursor3 = referenceValueCursor("projects/p1/databases/d1/documents/c1/doc2/c2/doc2");

    PartitionQueryPair partitionQueryPair =
        new PartitionQueryPair(
            PartitionQueryRequest.newBuilder().setStructuredQuery(query).build(),
            PartitionQueryResponse.newBuilder()
                .addPartitions(cursor3)
                .addPartitions(cursor1)
                .addPartitions(cursor2)
                .build());

    ArgumentCaptor<RunQueryRequest> captor = ArgumentCaptor.forClass(RunQueryRequest.class);
    when(processContext.element()).thenReturn(partitionQueryPair);
    doNothing().when(processContext).output(captor.capture());

    PartitionQueryResponseToRunQueryRequest fn = new PartitionQueryResponseToRunQueryRequest(now);
    fn.processElement(processContext);

    List<RunQueryRequest> result = captor.getAllValues();
    assertEquals(4, result.size());
    result.forEach(r -> assertEquals(Timestamps.fromMillis(now.getMillis()), r.getReadTime()));
  }

  private static Cursor referenceValueCursor(String referenceValue) {
    return Cursor.newBuilder()
        .addValues(Value.newBuilder().setReferenceValue(referenceValue).build())
        .build();
  }

  private static StructuredQuery newQueryWithCursors(
      StructuredQuery query, Cursor startAt, Cursor endAt) {
    StructuredQuery.Builder builder = query.toBuilder();
    if (startAt != null) {
      builder.setStartAt(startAt);
    }
    if (endAt != null) {
      builder.setEndAt(endAt);
    }
    return builder.build();
  }
}
