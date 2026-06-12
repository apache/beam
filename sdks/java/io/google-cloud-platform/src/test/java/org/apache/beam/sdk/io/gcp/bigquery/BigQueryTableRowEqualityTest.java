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

import static org.apache.beam.sdk.io.gcp.bigquery.TableRowMatchers.isTableRowEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link TableRowMatchers} class. */
@RunWith(JUnit4.class)
public class BigQueryTableRowEqualityTest {

  @Test
  public void testIdenticalRows() {
    TableRow row1 = new TableRow().set("count", 1).set("name", "Alice");
    TableRow row2 = new TableRow().set("count", 1).set("name", "Alice");
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testEmptyRows() {
    TableRow row1 = new TableRow();
    TableRow row2 = new TableRow();
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testIntegerVsString() {
    TableRow rowWithInteger = new TableRow().set("count", 1);
    TableRow rowWithString = new TableRow().set("count", "1");
    assertThat(rowWithInteger, not(isTableRowEqualTo(rowWithString)));
  }

  @Test
  public void testDoubleVsInteger() {
    TableRow rowWithDouble = new TableRow().set("value", 1.0);
    TableRow rowWithInteger = new TableRow().set("value", 1);
    assertThat(rowWithDouble, not(isTableRowEqualTo(rowWithInteger)));
  }

  @Test
  public void testBooleanVsString() {
    TableRow rowWithBoolean = new TableRow().set("active", true);
    TableRow rowWithString = new TableRow().set("active", "true");
    assertThat(rowWithBoolean, not(isTableRowEqualTo(rowWithString)));
  }

  @Test
  public void testBothFieldsNull() {
    TableRow row1 = new TableRow().set("name", null);
    TableRow row2 = new TableRow().set("name", null);
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testNullVsNonNull() {
    TableRow rowWithNull = new TableRow().set("name", null);
    TableRow rowWithValue = new TableRow().set("name", "Alice");
    assertThat(rowWithNull, not(isTableRowEqualTo(rowWithValue)));
  }

  @Test
  public void testEmptyStringVsNull() {
    TableRow rowWithEmptyString = new TableRow().set("name", "");
    TableRow rowWithNull = new TableRow().set("name", null);
    assertThat(rowWithEmptyString, not(isTableRowEqualTo(rowWithNull)));
  }

  @Test
  public void testWhitespaceDifference() {
    TableRow rowWithoutSpace = new TableRow().set("name", "Alice");
    TableRow rowWithLeadingSpace = new TableRow().set("name", " Alice");
    assertThat(rowWithoutSpace, not(isTableRowEqualTo(rowWithLeadingSpace)));
  }

  @Test
  public void testDifferentFieldCount() {
    TableRow rowWithTwoFields = new TableRow().set("a", 1).set("b", 2);
    TableRow rowWithOneField = new TableRow().set("a", 1);
    assertThat(rowWithTwoFields, not(isTableRowEqualTo(rowWithOneField)));
  }

  @Test
  public void testMissingField() {
    TableRow rowWithFieldB = new TableRow().set("a", 1).set("b", 2);
    TableRow rowWithFieldC = new TableRow().set("a", 1).set("c", 2);
    assertThat(rowWithFieldB, not(isTableRowEqualTo(rowWithFieldC)));
  }

  @Test
  public void testDifferentInsertionOrder() {
    TableRow row1 = new TableRow().set("a", 1).set("b", 2);
    TableRow row2 = new TableRow().set("b", 2).set("a", 1);
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testIdenticalNestedRows() {
    TableRow innerRow1 = new TableRow().set("id", 42);
    TableRow innerRow2 = new TableRow().set("id", 42);
    TableRow outerRow1 = new TableRow().set("nested", innerRow1);
    TableRow outerRow2 = new TableRow().set("nested", innerRow2);
    assertThat(outerRow1, isTableRowEqualTo(outerRow2));
  }

  @Test
  public void testNestedRowsWithTypeMismatch() {
    TableRow innerRowWithInteger = new TableRow().set("id", 42);
    TableRow innerRowWithString = new TableRow().set("id", "42");
    TableRow outerRow1 = new TableRow().set("nested", innerRowWithInteger);
    TableRow outerRow2 = new TableRow().set("nested", innerRowWithString);
    assertThat(outerRow1, not(isTableRowEqualTo(outerRow2)));
  }

  @Test
  public void testDeeplyNestedRowsWithTypeMismatch() {
    TableRow level3WithInteger = new TableRow().set("val", 1);
    TableRow level3WithString = new TableRow().set("val", "1");
    TableRow level2Row1 = new TableRow().set("l2", level3WithInteger);
    TableRow level2Row2 = new TableRow().set("l2", level3WithString);
    TableRow level1Row1 = new TableRow().set("l1", level2Row1);
    TableRow level1Row2 = new TableRow().set("l1", level2Row2);
    assertThat(level1Row1, not(isTableRowEqualTo(level1Row2)));
  }

  @Test
  public void testIdenticalListFields() {
    TableRow row1 = new TableRow().set("tags", Arrays.asList("a", "b"));
    TableRow row2 = new TableRow().set("tags", Arrays.asList("a", "b"));
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testListFieldsWithDifferentOrder() {
    TableRow row1 = new TableRow().set("tags", Arrays.asList("a", "b"));
    TableRow row2 = new TableRow().set("tags", Arrays.asList("b", "a"));
    assertThat(row1, not(isTableRowEqualTo(row2)));
  }

  @Test
  public void testZeroIntegerVsZeroDouble() {
    TableRow rowWithZeroInteger = new TableRow().set("value", 0);
    TableRow rowWithZeroDouble = new TableRow().set("value", 0.0);
    assertThat(rowWithZeroInteger, not(isTableRowEqualTo(rowWithZeroDouble)));
  }

  @Test
  public void testNegativeNumbers() {
    TableRow row1 = new TableRow().set("temp", -10);
    TableRow row2 = new TableRow().set("temp", -10);
    assertThat(row1, isTableRowEqualTo(row2));
  }

  @Test
  public void testLongVsInteger() {
    TableRow rowWithLong = new TableRow().set("count", 1L);
    TableRow rowWithInteger = new TableRow().set("count", 1);
    assertThat(rowWithLong, not(isTableRowEqualTo(rowWithInteger)));
  }

  @Test
  public void testTrueVsFalse() {
    TableRow rowWithTrue = new TableRow().set("active", true);
    TableRow rowWithFalse = new TableRow().set("active", false);
    assertThat(rowWithTrue, not(isTableRowEqualTo(rowWithFalse)));
  }

  @Test
  public void testLargeIntegerVsLong() {
    TableRow rowWithInteger = new TableRow().set("big", Integer.MAX_VALUE);
    TableRow rowWithLong = new TableRow().set("big", (long) Integer.MAX_VALUE);
    assertThat(rowWithInteger, not(isTableRowEqualTo(rowWithLong)));
  }

  @Test
  public void testMultipleFieldsWithOneTypeMismatch() {
    TableRow row1 = new TableRow().set("id", 1).set("name", "Alice").set("score", 99);
    TableRow row2 = new TableRow().set("id", 1).set("name", "Alice").set("score", "99");
    assertThat(row1, not(isTableRowEqualTo(row2)));
  }
}
