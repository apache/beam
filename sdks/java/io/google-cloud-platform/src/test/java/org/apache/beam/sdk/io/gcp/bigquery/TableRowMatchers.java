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

import com.google.api.services.bigquery.model.TableRow;
import java.util.stream.Collectors;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class TableRowMatchers {

  static Matcher<TableRow> isTableRowEqualTo(TableRow expected) {
    return new TypeSafeMatcher<TableRow>() {

      @Override
      protected boolean matchesSafely(TableRow actual) {
        return rowsMatch(expected, actual);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("TableRow (strict) ");
        description.appendText(formatValue(expected, 0));
      }

      @Override
      protected void describeMismatchSafely(TableRow actual, Description mismatch) {
        describeRowMismatch(expected, actual, mismatch);
      }
    };
  }

  private static boolean rowsMatch(TableRow expected, TableRow actual) {
    if (expected == null && actual == null) {
      return true;
    }

    if (expected == null || actual == null) {
      return false;
    }

    if (actual.size() != expected.size()) {
      return false;
    }

    for (String key : expected.keySet()) {
      if (!actual.containsKey(key)) {
        return false;
      }

      Object expectedVal = expected.get(key);
      Object actualVal = actual.get(key);

      if (expectedVal == null && actualVal == null) {
        continue;
      }
      if (expectedVal == null || actualVal == null) {
        return false;
      }

      // recursively compare nested TableRows
      if (expectedVal instanceof TableRow && actualVal instanceof TableRow) {
        if (!rowsMatch((TableRow) expectedVal, (TableRow) actualVal)) {
          return false;
        }
        continue;
      }

      if (!expectedVal.getClass().equals(actualVal.getClass())) {
        return false;
      }

      if (!expectedVal.equals(actualVal)) {
        return false;
      }
    }
    return true;
  }

  private static void describeRowMismatch(
      TableRow expected, TableRow actual, Description mismatch) {

    // size mismatch
    if (actual.size() != expected.size()) {
      mismatch.appendText(
          String.format(
              "had %d field(s) %s but expected %d field(s) %s",
              actual.size(), actual.keySet(), expected.size(), expected.keySet()));
      return;
    }

    // missing field
    for (String key : expected.keySet()) {
      if (!actual.containsKey(key)) {
        mismatch.appendText(String.format("missing field '%s'", key));
        return;
      }
    }

    // value/type mismatch
    for (String key : expected.keySet()) {
      Object expectedVal = expected.get(key);
      Object actualVal = actual.get(key);

      if (expectedVal == null && actualVal == null) {
        continue;
      }
      ;

      if (expectedVal == null) {
        mismatch.appendText(
            String.format(
                "field '%s': expected null but was %s(%s)",
                key, actualVal.getClass().getSimpleName(), formatValue(actualVal, 0)));
        return;
      }

      if (actualVal == null) {
        mismatch.appendText(
            String.format(
                "field '%s': expected %s(%s) but was null",
                key, expectedVal.getClass().getSimpleName(), formatValue(expectedVal, 0)));
        return;
      }

      // recurse into nested TableRows
      if (expectedVal instanceof TableRow && actualVal instanceof TableRow) {
        if (!rowsMatch((TableRow) expectedVal, (TableRow) actualVal)) {
          mismatch.appendText(String.format("field '%s': ", key));
          describeRowMismatch((TableRow) expectedVal, (TableRow) actualVal, mismatch);
          return;
        }
        continue;
      }

      // type mismatch
      if (!expectedVal.getClass().equals(actualVal.getClass())) {
        mismatch.appendText(
            String.format(
                "field '%s': expected %s(%s) but was %s(%s)",
                key,
                expectedVal.getClass().getSimpleName(),
                formatValue(expectedVal, 0),
                actualVal.getClass().getSimpleName(),
                formatValue(actualVal, 0)));
        return;
      }

      // value mismatch
      if (!expectedVal.equals(actualVal)) {
        mismatch.appendText(
            String.format(
                "field '%s': expected value (%s) but was (%s)",
                key, formatValue(expectedVal, 0), formatValue(actualVal, 0)));
        return;
      }
    }
  }

  // recursive formatter
  private static String formatValue(Object val, int depth) {
    if (val == null) {
      return "null";
    }

    // safety net against infinite recursion
    if (depth > 10) {
      return "...";
    }
    if (val instanceof TableRow) {
      TableRow row = (TableRow) val;
      String fields =
          row.keySet().stream()
              .map(
                  k -> {
                    Object v = row.get(k);
                    String typeName =
                        v == null
                            ? "null"
                            : v instanceof TableRow ? "TableRow" : v.getClass().getSimpleName();
                    return String.format("%s(%s)=%s", k, typeName, formatValue(v, depth + 1));
                  })
              .collect(Collectors.joining(", "));
      return "TableRow{" + fields + "}";
    }
    return String.valueOf(val);
  }
}
