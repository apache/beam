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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.CompositeFilter;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldFilter;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Filter;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.StructuredQuery.UnaryFilter;
import com.google.firestore.v1.Value;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

public class QueryUtilsTest {

  private Document testDocument;
  private StructuredQuery testQuery;

  @Before
  public void setUp() {
    // { "__name__": "doc-123", "fo`o.m`ap": { "bar.key": "bar.val" } }
    testDocument =
        Document.newBuilder()
            .setName("doc-123")
            .putAllFields(
                ImmutableMap.of(
                    "fo`o.m`ap",
                    Value.newBuilder()
                        .setMapValue(
                            MapValue.newBuilder()
                                .putFields(
                                    "bar.key", Value.newBuilder().setStringValue("bar.val").build())
                                .build())
                        .build()))
            .build();

    // WHERE (`z€a`.a.a != "" AND `b` > "") AND c == "" AND `z$` > "456" AND `z` > "123" AND z IS
    // NOT NAN
    Filter.Builder filter =
        Filter.newBuilder()
            .setCompositeFilter(
                CompositeFilter.newBuilder()
                    .addFilters(
                        Filter.newBuilder()
                            .setCompositeFilter(
                                CompositeFilter.newBuilder()
                                    .addFilters(
                                        Filter.newBuilder()
                                            .setFieldFilter(
                                                FieldFilter.newBuilder()
                                                    .setField(
                                                        FieldReference.newBuilder()
                                                            .setFieldPath("`z€a`.a.a"))
                                                    .setOp(FieldFilter.Operator.NOT_EQUAL)
                                                    .setValue(
                                                        Value.newBuilder().setStringValue(""))))
                                    .addFilters(
                                        Filter.newBuilder()
                                            .setFieldFilter(
                                                FieldFilter.newBuilder()
                                                    .setField(
                                                        FieldReference.newBuilder()
                                                            .setFieldPath("`b`"))
                                                    .setOp(FieldFilter.Operator.GREATER_THAN)
                                                    .setValue(
                                                        Value.newBuilder().setStringValue(""))))
                                    .setOp(CompositeFilter.Operator.AND)))
                    .addFilters(
                        Filter.newBuilder()
                            .setFieldFilter(
                                FieldFilter.newBuilder()
                                    .setField(FieldReference.newBuilder().setFieldPath("c"))
                                    .setOp(FieldFilter.Operator.EQUAL)
                                    .setValue(Value.newBuilder().setStringValue(""))))
                    .addFilters(
                        Filter.newBuilder()
                            .setFieldFilter(
                                FieldFilter.newBuilder()
                                    .setField(FieldReference.newBuilder().setFieldPath("`z$`"))
                                    .setOp(FieldFilter.Operator.GREATER_THAN)
                                    .setValue(Value.newBuilder().setStringValue("456"))))
                    .addFilters(
                        Filter.newBuilder()
                            .setFieldFilter(
                                FieldFilter.newBuilder()
                                    .setField(FieldReference.newBuilder().setFieldPath("`z`"))
                                    .setOp(FieldFilter.Operator.GREATER_THAN)
                                    .setValue(Value.newBuilder().setStringValue("123"))))
                    .addFilters(
                        Filter.newBuilder()
                            .setUnaryFilter(
                                UnaryFilter.newBuilder()
                                    .setField(FieldReference.newBuilder().setFieldPath("z"))
                                    .setOp(UnaryFilter.Operator.IS_NOT_NAN)))
                    .setOp(CompositeFilter.Operator.AND)
                    .build());
    testQuery =
        StructuredQuery.newBuilder()
            .addFrom(
                CollectionSelector.newBuilder()
                    .setAllDescendants(false)
                    .setCollectionId("collection"))
            .setWhere(filter)
            .addOrderBy(
                Order.newBuilder()
                    .setField(FieldReference.newBuilder().setFieldPath("b"))
                    .setDirection(Direction.DESCENDING))
            .build();
  }

  @Test
  public void getImplicitOrderBy_success() {
    // WHERE (`z€a`.a.a != "" AND `b` > "") AND c == "" AND `z$` > "456" AND `z` > "123" AND z IS
    // NOT NAN ORDER BY b DESC
    // -> (ORDER BY b DESC) + `z` DESC, `z$` DESC, `z€a`.a.a DESC, __name__ DESC
    List<Order> expected =
        ImmutableList.of(
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z`"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z$`"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z€a`.a.a"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                .setDirection(Direction.DESCENDING)
                .build());
    List<Order> actual = QueryUtils.getImplicitOrderBy(testQuery);
    assertEquals(expected, actual);
  }

  @Test
  public void getImplicitOrderBy_nameInWhere() {
    StructuredQuery.Builder builder = testQuery.toBuilder();
    builder
        .getWhereBuilder()
        .getCompositeFilterBuilder()
        .addFilters(
            Filter.newBuilder()
                .setFieldFilter(
                    FieldFilter.newBuilder()
                        .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                        .setOp(FieldFilter.Operator.NOT_EQUAL)
                        .setValue(Value.newBuilder().setStringValue(""))));
    testQuery = builder.build();
    // WHERE (`z€a`.a.a != "" AND `b` > "") AND c == "" AND `z$` > "456" AND `z` > "123" AND z IS
    // NOT NAN AND __name__ != "" ORDER BY b DESC
    // -> (ORDER BY b DESC) + __name__ DESC, `z` DESC, `z$` DESC, `z€a`.a.a DESC
    List<Order> expected =
        ImmutableList.of(
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z`"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z$`"))
                .setDirection(Direction.DESCENDING)
                .build(),
            Order.newBuilder()
                .setField(FieldReference.newBuilder().setFieldPath("`z€a`.a.a"))
                .setDirection(Direction.DESCENDING)
                .build());
    List<Order> actual = QueryUtils.getImplicitOrderBy(testQuery);
    assertEquals(expected, actual);
  }

  @Test
  public void getImplicitOrderBy_malformedWhereThrows() {
    testQuery =
        testQuery
            .toBuilder()
            .setWhere(
                Filter.newBuilder()
                    .setUnaryFilter(
                        UnaryFilter.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath(""))
                            .setOp(UnaryFilter.Operator.IS_NOT_NAN)))
            .build();
    assertThrows(IllegalArgumentException.class, () -> QueryUtils.getImplicitOrderBy(testQuery));
  }

  @Test
  public void lookupDocumentValue_findsName() {
    assertEquals(
        QueryUtils.lookupDocumentValue(testDocument, "__name__"),
        Value.newBuilder().setReferenceValue("doc-123").build());
  }

  @Test
  public void lookupDocumentValue_nestedField() {
    assertEquals(
        QueryUtils.lookupDocumentValue(testDocument, "`fo\\`o.m\\`ap`.`bar.key`"),
        Value.newBuilder().setStringValue("bar.val").build());
  }

  @Test
  public void lookupDocumentValue_returnsNullIfNotFound() {
    assertNull(QueryUtils.lookupDocumentValue(testDocument, "foobar"));
  }

  @Test
  public void lookupDocumentValue_invalidThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> QueryUtils.lookupDocumentValue(testDocument, ""));
  }
}
