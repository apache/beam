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
package org.apache.beam.sdk.extensions.sql.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.Iterables;

/** Contain helpers to assert {@link Row}s. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RowAsserts {

  /** Asserts result contains single row with an int field. */
  public static SerializableFunction<Iterable<Row>, Void> matchesScalar(int expected) {
    return records -> {
      Row row = Iterables.getOnlyElement(records);
      assertNotNull(row);
      assertEquals(expected, (int) row.getInt32(0));
      return null;
    };
  }

  /** Asserts result contains single row with an long field. */
  public static SerializableFunction<Iterable<Row>, Void> matchesScalar(long expected) {
    return records -> {
      Row row = Iterables.getOnlyElement(records);
      assertNotNull(row);
      assertEquals(expected, (long) row.getInt64(0));
      return null;
    };
  }

  /** Asserts result contains single row with a double field. */
  public static SerializableFunction<Iterable<Row>, Void> matchesScalar(
      double expected, double delta) {

    return input -> {
      Row row = Iterables.getOnlyElement(input);
      assertNotNull(row);
      assertEquals(expected, row.getDouble(0), delta);
      return null;
    };
  }

  public static SerializableFunction<Iterable<Row>, Void> matchesScalar(
      float expected, float delta) {

    return input -> {
      Row row = Iterables.getOnlyElement(input);
      assertNotNull(row);
      assertEquals(expected, row.getFloat(0), delta);
      return null;
    };
  }
}
