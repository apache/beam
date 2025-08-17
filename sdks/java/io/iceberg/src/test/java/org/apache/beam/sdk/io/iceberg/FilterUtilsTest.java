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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.FilterUtils.convert;
import static org.apache.beam.sdk.io.iceberg.FilterUtils.getReferencedFieldNames;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.util.DateTimeUtil.daysFromDate;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTime;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test class for {@link FilterUtils}. */
public class FilterUtilsTest {
  @Test
  public void testIsNull() {
    TestCase.expecting(isNull("fiELd_1"))
        .fromFilter("\"fiELd_1\" IS NULL")
        .withFieldType(Types.IntegerType.get())
        .validate();
  }

  @Test
  public void testIsNotNull() {
    TestCase.expecting(notNull("fiELd_1"))
        .fromFilter("\"fiELd_1\" IS NOT NULL")
        .withFieldType(Types.IntegerType.get())
        .validate();
  }

  @Test
  public void testLessThan() {
    // integer
    TestCase.expecting(lessThan("field_1", 30))
        .fromFilter("\"field_1\" < 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(lessThan("field_1", 30.58f))
        .fromFilter("\"field_1\" < 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(lessThan("field_1", "xyz"))
        .fromFilter("\"field_1\" < 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(lessThan("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" < '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(lessThan("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" < DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(lessThan("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" < '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(lessThan("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" < TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            lessThan(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" < '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            lessThan(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" < TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            lessThan("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" < '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            lessThan("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" < DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testLessThanOrEqual() {
    // integer
    TestCase.expecting(lessThanOrEqual("field_1", 30))
        .fromFilter("\"field_1\" <= 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(lessThanOrEqual("field_1", 30.58f))
        .fromFilter("\"field_1\" <= 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(lessThanOrEqual("field_1", "xyz"))
        .fromFilter("\"field_1\" <= 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(lessThanOrEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" <= '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(lessThanOrEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" <= DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(lessThanOrEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" <= '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(lessThanOrEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" <= TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            lessThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" <= '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            lessThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" <= TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            lessThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" <= '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            lessThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" <= DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testGreaterThan() {
    // integer
    TestCase.expecting(greaterThan("field_1", 30))
        .fromFilter("\"field_1\" > 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(greaterThan("field_1", 30.58f))
        .fromFilter("\"field_1\" > 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(greaterThan("field_1", "xyz"))
        .fromFilter("\"field_1\" > 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(greaterThan("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" > '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(greaterThan("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" > DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(greaterThan("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" > '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(greaterThan("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" > TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            greaterThan(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" > '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            greaterThan(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" > TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            greaterThan("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" > '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            greaterThan("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" > DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testGreaterThanOrEqual() {
    // integer
    TestCase.expecting(greaterThanOrEqual("field_1", 30))
        .fromFilter("\"field_1\" >= 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(greaterThanOrEqual("field_1", 30.58f))
        .fromFilter("\"field_1\" >= 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(greaterThanOrEqual("field_1", "xyz"))
        .fromFilter("\"field_1\" >= 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(greaterThanOrEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" >= '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(greaterThanOrEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" >= DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(
            greaterThanOrEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" >= '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(
            greaterThanOrEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" >= TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            greaterThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" >= '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            greaterThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" >= TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            greaterThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" >= '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            greaterThanOrEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" >= DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testEquals() {
    // integer
    TestCase.expecting(equal("field_1", 30))
        .fromFilter("\"field_1\" = 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(equal("field_1", 30.58f))
        .fromFilter("\"field_1\" = 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(equal("field_1", "xyz"))
        .fromFilter("\"field_1\" = 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(equal("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" = '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(equal("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" = DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(equal("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" = '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(equal("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" = TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            equal("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" = '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            equal("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" = TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            equal("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" = '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            equal("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" = DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testNotEquals() {
    // integer
    TestCase.expecting(notEqual("field_1", 30))
        .fromFilter("\"field_1\" <> 30")
        .withFieldType(Types.IntegerType.get())
        .validate();

    // float
    TestCase.expecting(notEqual("field_1", 30.58f))
        .fromFilter("\"field_1\" <> 30.58")
        .withFieldType(Types.FloatType.get())
        .validate();

    // string
    TestCase.expecting(notEqual("field_1", "xyz"))
        .fromFilter("\"field_1\" <> 'xyz'")
        .withFieldType(Types.StringType.get())
        .validate();

    // date string
    TestCase.expecting(notEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" <> '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // date
    TestCase.expecting(notEqual("field_1", daysFromDate(LocalDate.parse("2025-05-03"))))
        .fromFilter("\"field_1\" <> DATE '2025-05-03'")
        .withFieldType(Types.DateType.get())
        .validate();

    // time string
    TestCase.expecting(notEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" <> '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // time
    TestCase.expecting(notEqual("field_1", microsFromTime(LocalTime.parse("10:30:05.123"))))
        .fromFilter("\"field_1\" <> TIME '10:30:05.123'")
        .withFieldType(Types.TimeType.get())
        .validate();

    // datetime - timestamp string
    TestCase.expecting(
            notEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" <> '2025-05-03T10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - timestamp
    TestCase.expecting(
            notEqual(
                "field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123"))))
        .fromFilter("\"field_1\" <> TIMESTAMP '2025-05-03 10:30:05.123'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date string
    TestCase.expecting(
            notEqual("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" <> '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();

    // datetime - date
    TestCase.expecting(
            notEqual("field_1", microsFromTimestamp(LocalDateTime.parse("2025-05-03T00:00:00"))))
        .fromFilter("\"field_1\" <> DATE '2025-05-03'")
        .withFieldType(Types.TimestampType.withoutZone())
        .validate();
  }

  @Test
  public void testIn() {
    // string
    TestCase.expecting(in("field_1", Arrays.asList("xyz", "abc", "123", "foo")))
        .fromFilter("\"field_1\" IN ('xyz', 'abc', '123', 'foo')")
        .withFieldType(Types.StringType.get())
        .validate();

    // integer
    TestCase.expecting(in("field_1", Arrays.asList(1, 2, 3, 4, 5)))
        .fromFilter("\"field_1\" IN (1, 2, 3, 4, 5)")
        .withFieldType(Types.IntegerType.get())
        .validate();
  }

  @Test
  public void testNotIn() {
    // string
    TestCase.expecting(notIn("field_1", Arrays.asList("xyz", "abc", "123", "foo")))
        .fromFilter("\"field_1\" NOT IN ('xyz', 'abc', '123', 'foo')")
        .withFieldType(Types.StringType.get())
        .validate();

    // integer
    TestCase.expecting(notIn("field_1", Arrays.asList(1, 2, 3, 4, 5)))
        .fromFilter("\"field_1\" NOT IN (1, 2, 3, 4, 5)")
        .withFieldType(Types.IntegerType.get())
        .validate();
  }

  @Test
  public void testAnd() {
    TestCase.expecting(
            and(
                and(
                    and(
                        and(
                            and(
                                and(
                                    and(and(and(IS_NULL, NOT_NULL), LESS_THAN), LESS_THAN_OR_EQUAL),
                                    GREATER_THAN),
                                GREATER_THAN_OR_EQUAL),
                            EQUAL),
                        NOT_EQUAL),
                    IN),
                NOT_IN))
        .fromFilter(
            "\"field_1\" IS NULL AND "
                + "\"field_2\" IS NOT NULL AND "
                + "\"field_3\" < 'xyz' AND "
                + "\"field_4\" <= 123 AND "
                + "\"field_5\" > 123.456 AND "
                + "\"field_6\" >= '2025-05-03' AND "
                + "\"field_7\" = '10:30:05.123' AND "
                + "\"field_8\" <> '2025-05-03T10:30:05.123' AND "
                + "\"field_9\" IN ('xyz', 'abc', '123', 'foo') AND "
                + "\"field_10\" NOT IN (1, 2, 3, 4, 5)")
        .withSchema(SCHEMA)
        .validate();
  }

  @Test
  public void testOr() {
    TestCase.expecting(
            or(
                or(
                    or(
                        or(
                            or(
                                or(
                                    or(or(or(IS_NULL, NOT_NULL), LESS_THAN), LESS_THAN_OR_EQUAL),
                                    GREATER_THAN),
                                GREATER_THAN_OR_EQUAL),
                            EQUAL),
                        NOT_EQUAL),
                    IN),
                NOT_IN))
        .fromFilter(
            "\"field_1\" IS NULL OR "
                + "\"field_2\" IS NOT NULL OR "
                + "\"field_3\" < 'xyz' OR "
                + "\"field_4\" <= 123 OR "
                + "\"field_5\" > 123.456 OR "
                + "\"field_6\" >= '2025-05-03' OR "
                + "\"field_7\" = '10:30:05.123' OR "
                + "\"field_8\" <> '2025-05-03T10:30:05.123' OR "
                + "\"field_9\" IN ('xyz', 'abc', '123', 'foo') OR "
                + "\"field_10\" NOT IN (1, 2, 3, 4, 5)")
        .withSchema(SCHEMA)
        .validate();
  }

  @Test
  public void testAndOr() {
    TestCase.expecting(
            or(
                or(
                    or(
                        or(and(IS_NULL, NOT_NULL), and(LESS_THAN, LESS_THAN_OR_EQUAL)),
                        and(GREATER_THAN, GREATER_THAN_OR_EQUAL)),
                    and(EQUAL, NOT_EQUAL)),
                and(IN, NOT_IN)))
        .fromFilter(
            "\"field_1\" IS NULL AND "
                + "\"field_2\" IS NOT NULL OR "
                + "\"field_3\" < 'xyz' AND "
                + "\"field_4\" <= 123 OR "
                + "\"field_5\" > 123.456 AND "
                + "\"field_6\" >= '2025-05-03' OR "
                + "\"field_7\" = '10:30:05.123' AND "
                + "\"field_8\" <> '2025-05-03T10:30:05.123' OR "
                + "\"field_9\" IN ('xyz', 'abc', '123', 'foo') AND "
                + "\"field_10\" NOT IN (1, 2, 3, 4, 5)")
        .withSchema(SCHEMA)
        .validate();
  }

  @Test
  public void testScanFiles() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()), required(2, "str", Types.StringType.get()));
    Table table = warehouse.createTable(TableIdentifier.parse("default.table"), schema);

    List<Record> recs =
        IntStream.range(0, 100)
            .mapToObj(
                i -> {
                  GenericRecord rec = GenericRecord.create(schema);
                  rec.setField("id", i);
                  rec.setField("str", "value_" + i);
                  return rec;
                })
            .collect(Collectors.toList());

    ImmutableList<DataFile> files =
        ImmutableList.of(
            warehouse.writeRecords("file_0.parquet", schema, recs.subList(0, 10)),
            warehouse.writeRecords("file_10.parquet", schema, recs.subList(10, 20)),
            warehouse.writeRecords("file_20.parquet", schema, recs.subList(20, 30)),
            warehouse.writeRecords("file_30.parquet", schema, recs.subList(30, 40)),
            warehouse.writeRecords("file_40.parquet", schema, recs.subList(40, 50)),
            warehouse.writeRecords("file_50.parquet", schema, recs.subList(50, 60)),
            warehouse.writeRecords("file_60.parquet", schema, recs.subList(60, 70)),
            warehouse.writeRecords("file_70.parquet", schema, recs.subList(70, 80)),
            warehouse.writeRecords("file_80.parquet", schema, recs.subList(80, 90)),
            warehouse.writeRecords("file_90.parquet", schema, recs.subList(90, 100)));

    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();

    TableScan scan =
        table.newScan().project(schema).filter(FilterUtils.convert("\"id\" < 58", schema));

    Set<String> expectedFiles =
        ImmutableSet.of(
            "file_0.parquet",
            "file_10.parquet",
            "file_20.parquet",
            "file_30.parquet",
            "file_40.parquet",
            "file_50.parquet");
    ImmutableSet.Builder<String> actualFiles = ImmutableSet.builder();
    for (FileScanTask task : scan.planFiles()) {
      String fileName = Iterables.getLast(Splitter.on('/').split(task.file().path().toString()));
      actualFiles.add(fileName);
    }
    assertEquals(expectedFiles, actualFiles.build());
  }

  @Test
  public void testReferencedFieldsInFilter() {
    List<Pair<String, Set<String>>> cases =
        Arrays.asList(
            Pair.of("field_1 < 35", Sets.newHashSet("FIELD_1")),
            Pair.of("\"field_1\" in (1, 2, 3)", Sets.newHashSet("field_1")),
            Pair.of("field_1 < 35 and \"fiELd_2\" = TRUE", Sets.newHashSet("FIELD_1", "fiELd_2")),
            Pair.of(
                "(\"field_1\" < 35 and \"field_2\" = TRUE) or \"field_3\" in ('a', 'b')",
                Sets.newHashSet("field_1", "field_2", "field_3")));

    for (Pair<String, Set<String>> pair : cases) {
      assertEquals(pair.getRight(), getReferencedFieldNames(pair.getLeft()));
    }
  }

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private static class TestCase {
    private @Nullable String filter;
    private @Nullable Schema schema;
    private final Expression expected;

    private TestCase(Expression expression) {
      this.expected = expression;
    }

    TestCase fromFilter(String filter) {
      this.filter = filter;
      return this;
    }

    TestCase withFieldType(Type type) {
      String fieldName = ((UnboundPredicate<?>) expected).ref().name();
      this.schema = new Schema(Types.NestedField.required(1, fieldName, type));
      return this;
    }

    TestCase withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    static TestCase expecting(Expression expression) {
      return new TestCase(expression);
    }

    void validate() {
      Preconditions.checkState(
          schema != null && filter != null, "TestCase has not been fully initialized yet");
      Expression actual = convert(filter, schema);
      checkEquals(expected, actual);
    }
  }

  private static final Expression IS_NULL = isNull("field_1");
  private static final Expression NOT_NULL = notNull("field_2");
  private static final Expression LESS_THAN = lessThan("field_3", "xyz");
  private static final Expression LESS_THAN_OR_EQUAL = lessThanOrEqual("field_4", 123);
  private static final Expression GREATER_THAN = greaterThan("field_5", 123.456f);
  private static final Expression GREATER_THAN_OR_EQUAL =
      greaterThanOrEqual("field_6", daysFromDate(LocalDate.parse("2025-05-03")));
  private static final Expression EQUAL =
      equal("field_7", microsFromTime(LocalTime.parse("10:30:05.123")));
  private static final Expression NOT_EQUAL =
      notEqual("field_8", microsFromTimestamp(LocalDateTime.parse("2025-05-03T10:30:05.123")));
  private static final Expression IN = in("field_9", Arrays.asList("xyz", "abc", "123", "foo"));
  private static final Expression NOT_IN = notIn("field_10", Arrays.asList(1, 2, 3, 4, 5));

  private static final Schema SCHEMA =
      new Schema(
          required(1, "field_1", Types.StringType.get()),
          required(2, "field_2", Types.StringType.get()),
          required(3, "field_3", Types.StringType.get()),
          required(4, "field_4", Types.IntegerType.get()),
          required(5, "field_5", Types.FloatType.get()),
          required(6, "field_6", Types.DateType.get()),
          required(7, "field_7", Types.TimeType.get()),
          required(8, "field_8", Types.TimestampType.withoutZone()),
          required(9, "field_9", Types.StringType.get()),
          required(10, "field_10", Types.IntegerType.get()));

  private static void checkEquals(Expression expectedExpr, Expression actualExpr) {
    if (expectedExpr instanceof UnboundPredicate) {
      assertTrue(actualExpr instanceof UnboundPredicate);
    } else if (expectedExpr instanceof And) {
      assertTrue(actualExpr instanceof And);
      checkEqualsAnd((And) expectedExpr, (And) actualExpr);
      return;
    } else if (expectedExpr instanceof Or) {
      assertTrue(actualExpr instanceof Or);
      checkEqualsOr((Or) expectedExpr, (Or) actualExpr);
      return;
    }

    UnboundPredicate<?> expected = (UnboundPredicate<?>) expectedExpr;
    UnboundPredicate<?> actual = (UnboundPredicate<?>) actualExpr;
    assertEquals(expected.op(), actual.op());
    assertEquals(expected.ref().name(), actual.ref().name());

    ImmutableSet<Operation> inOperations = ImmutableSet.of(Operation.IN, Operation.NOT_IN);
    if (inOperations.contains(expected.op())) {
      System.out.printf(
          "xxx op: %s, literals: %s, ref: %s%n",
          expected.op(), expected.literals(), expected.ref().name());
      assertEquals(expected.literals(), actual.literals());
    } else {
      System.out.printf(
          "xxx op: %s, literal: %s, ref: %s%n",
          expected.op(), expected.literal(), expected.ref().name());
      assertEquals(expected.literal(), actual.literal());
    }
  }

  private static void checkEqualsAnd(And expected, And actual) {
    assertEquals(expected.op(), actual.op());
    checkEquals(expected.left(), actual.left());
    checkEquals(expected.right(), actual.right());
  }

  private static void checkEqualsOr(Or expected, Or actual) {
    assertEquals(expected.op(), actual.op());
    checkEquals(expected.left(), actual.left());
    checkEquals(expected.right(), actual.right());
  }
}
