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
package org.apache.beam.sdk.io.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Comprehensive unit tests for {@link ParquetFilterFactory}. */
@RunWith(JUnit4.class)
public class ParquetFilterFactoryTest {

  private Schema testSchema;

  @Before
  public void setUp() {
    testSchema =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addBooleanField("active")
            .addDoubleField("price")
            .addFloatField("rating")
            .addInt64Field("timestamp")
            .addByteArrayField("data")
            .build();
  }

  @Test
  public void testCreateWithNullExpressions() {
    ParquetFilter filter = ParquetFilterFactory.create(null, testSchema);
    assertNotNull(filter);
    assertNull(((ParquetFilterFactory.ParquetFilterImpl) filter).getPredicate());
  }

  @Test
  public void testCreateWithEmptyExpressions() {
    ParquetFilter filter = ParquetFilterFactory.create(Collections.emptyList(), testSchema);
    assertNotNull(filter);
    assertNull(((ParquetFilterFactory.ParquetFilterImpl) filter).getPredicate());
  }

  @Test
  public void testCreateWithValidSchema() {
    // Test that the factory can be created with a valid schema
    ParquetFilter filter = ParquetFilterFactory.create(Collections.emptyList(), testSchema);
    assertNotNull(filter);
    assertNull(((ParquetFilterFactory.ParquetFilterImpl) filter).getPredicate());
  }

  @Test
  public void testCreateWithInvalidSchema() {
    // Test that the factory throws an exception with null schema
    try {
      ParquetFilterFactory.create(Collections.emptyList(), null);
      assertTrue("Expected IllegalArgumentException", false);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Beam schema cannot be null"));
    }
  }

  @Test
  public void testFromPredicate() {
    IntColumn intColumn = org.apache.parquet.filter2.predicate.FilterApi.intColumn("test");
    FilterPredicate parquetPredicate =
        org.apache.parquet.filter2.predicate.FilterApi.eq(intColumn, 42);

    ParquetFilter filter = ParquetFilterFactory.fromPredicate(parquetPredicate);

    assertNotNull(filter);
    FilterPredicate retrievedPredicate =
        ((ParquetFilterFactory.ParquetFilterImpl) filter).getPredicate();
    assertEquals(parquetPredicate, retrievedPredicate);
  }
}
