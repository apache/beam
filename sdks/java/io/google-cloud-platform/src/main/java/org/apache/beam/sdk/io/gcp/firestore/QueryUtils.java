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

import com.google.firestore.v1.Document;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldFilter;
import com.google.firestore.v1.StructuredQuery.FieldFilter.Operator;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Filter;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.StructuredQuery.UnaryFilter;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Value.ValueTypeCase;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ascii;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Contains several internal utility functions for Firestore query handling, such as filling
 * implicit ordering or escaping field references.
 */
class QueryUtils {

  private static final ImmutableSet<Operator> INEQUALITY_FIELD_FILTER_OPS =
      ImmutableSet.of(
          FieldFilter.Operator.LESS_THAN,
          FieldFilter.Operator.LESS_THAN_OR_EQUAL,
          FieldFilter.Operator.GREATER_THAN,
          FieldFilter.Operator.GREATER_THAN_OR_EQUAL,
          FieldFilter.Operator.NOT_EQUAL,
          FieldFilter.Operator.NOT_IN);
  private static final ImmutableSet<UnaryFilter.Operator> INEQUALITY_UNARY_FILTER_OPS =
      ImmutableSet.of(UnaryFilter.Operator.IS_NOT_NAN, UnaryFilter.Operator.IS_NOT_NULL);

  /**
   * Populates implicit orderBy of a query in accordance with our documentation. * Required
   * inequality fields are appended in field name order. * __name__ is appended if not specified.
   * See <a
   * href=https://github.com/googleapis/googleapis/tree/master/google/firestore/v1/query.proto#L254>here</a>
   * for more details.
   *
   * @param query The StructuredQuery of the original request.
   * @return A list of additional orderBy fields, excluding the explicit ones.
   */
  static List<Order> getImplicitOrderBy(StructuredQuery query) {
    List<OrderByFieldPath> expectedImplicitOrders = new ArrayList<>();
    if (query.hasWhere()) {
      fillInequalityFields(query.getWhere(), expectedImplicitOrders);
    }
    Collections.sort(expectedImplicitOrders);
    if (expectedImplicitOrders.stream().noneMatch(OrderByFieldPath::isDocumentName)) {
      expectedImplicitOrders.add(OrderByFieldPath.fromString("__name__"));
    }
    for (Order order : query.getOrderByList()) {
      OrderByFieldPath orderField = OrderByFieldPath.fromString(order.getField().getFieldPath());
      expectedImplicitOrders.remove(orderField);
    }

    List<Order> additionalOrders = new ArrayList<>();
    if (!expectedImplicitOrders.isEmpty()) {
      Direction lastDirection =
          query.getOrderByCount() == 0
              ? Direction.ASCENDING
              : query.getOrderByList().get(query.getOrderByCount() - 1).getDirection();

      for (OrderByFieldPath field : expectedImplicitOrders) {
        additionalOrders.add(
            Order.newBuilder()
                .setDirection(lastDirection)
                .setField(
                    FieldReference.newBuilder().setFieldPath(field.getOriginalString()).build())
                .build());
      }
    }

    return additionalOrders;
  }

  private static void fillInequalityFields(Filter filter, List<OrderByFieldPath> result) {
    switch (filter.getFilterTypeCase()) {
      case FIELD_FILTER:
        if (INEQUALITY_FIELD_FILTER_OPS.contains(filter.getFieldFilter().getOp())) {
          OrderByFieldPath fieldPath =
              OrderByFieldPath.fromString(filter.getFieldFilter().getField().getFieldPath());
          if (!result.contains(fieldPath)) {
            result.add(fieldPath);
          }
        }
        break;
      case COMPOSITE_FILTER:
        filter.getCompositeFilter().getFiltersList().forEach(f -> fillInequalityFields(f, result));
        break;
      case UNARY_FILTER:
        if (INEQUALITY_UNARY_FILTER_OPS.contains(filter.getUnaryFilter().getOp())) {
          OrderByFieldPath fieldPath =
              OrderByFieldPath.fromString(filter.getUnaryFilter().getField().getFieldPath());
          if (!result.contains(fieldPath)) {
            result.add(fieldPath);
          }
        }
        break;
      default:
        break;
    }
  }

  static @Nullable Value lookupDocumentValue(Document document, String fieldPath) {
    OrderByFieldPath resolvedPath = OrderByFieldPath.fromString(fieldPath);
    // __name__ is a special field and doesn't exist in (top-level) valueMap (see
    // https://firebase.google.com/docs/firestore/reference/rest/v1/projects.databases.documents#Document).
    if (resolvedPath.isDocumentName()) {
      return Value.newBuilder().setReferenceValue(document.getName()).build();
    }
    return findMapValue(new ArrayList<>(resolvedPath.getSegments()), document.getFieldsMap());
  }

  private static @Nullable Value findMapValue(List<String> segments, Map<String, Value> valueMap) {
    if (segments.isEmpty()) {
      return null;
    }
    String field = segments.remove(0);
    Value value = valueMap.get(field);
    if (segments.isEmpty()) {
      return value;
    }
    // Field path traversal is not done, recurse into map values.
    if (value == null || !value.getValueTypeCase().equals(ValueTypeCase.MAP_VALUE)) {
      return null;
    }
    return findMapValue(segments, value.getMapValue().getFieldsMap());
  }

  private static class OrderByFieldPath implements Comparable<OrderByFieldPath> {

    private static final String UNQUOTED_NAME_REGEX_STRING = "([a-zA-Z_][a-zA-Z_0-9]*)";
    private static final String QUOTED_NAME_REGEX_STRING = "(`(?:[^`\\\\]|(?:\\\\.))+`)";
    // After each segment follows a dot and more characters, or the end of the string.
    private static final Pattern FIELD_PATH_SEGMENT_REGEX =
        Pattern.compile(
            String.format(
                "(?:%s|%s)(\\..+|$)", UNQUOTED_NAME_REGEX_STRING, QUOTED_NAME_REGEX_STRING),
            Pattern.DOTALL);

    public static OrderByFieldPath fromString(String fieldPath) {
      if (fieldPath.isEmpty()) {
        throw new IllegalArgumentException("Could not resolve empty field path");
      }
      String originalString = fieldPath;
      List<String> segments = new ArrayList<>();
      while (!fieldPath.isEmpty()) {
        Matcher segmentMatcher = FIELD_PATH_SEGMENT_REGEX.matcher(fieldPath);
        boolean foundMatch = segmentMatcher.lookingAt();
        if (!foundMatch) {
          throw new IllegalArgumentException("OrderBy field path was malformed");
        }
        String fieldName;
        if ((fieldName = segmentMatcher.group(1)) != null) {
          segments.add(fieldName);
        } else if ((fieldName = segmentMatcher.group(2)) != null) {
          String unescaped = unescapeFieldName(fieldName.substring(1, fieldName.length() - 1));
          segments.add(unescaped);
        } else {
          throw new IllegalArgumentException("OrderBy field path was malformed");
        }
        fieldPath = fieldPath.substring(fieldName.length());
        // Due to the regex, any non-empty fieldPath will have a dot before the next nested field.
        if (fieldPath.startsWith(".")) {
          fieldPath = fieldPath.substring(1);
        }
      }
      return new OrderByFieldPath(originalString, ImmutableList.copyOf(segments));
    }

    private final String originalString;
    private final ImmutableList<String> segments;

    private OrderByFieldPath(String originalString, ImmutableList<String> segments) {
      this.originalString = originalString;
      this.segments = segments;
    }

    public String getOriginalString() {
      return originalString;
    }

    public boolean isDocumentName() {
      return segments.size() == 1 && "__name__".equals(segments.get(0));
    }

    public ImmutableList<String> getSegments() {
      return segments;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof OrderByFieldPath) {
        return this.segments.equals(((OrderByFieldPath) other).getSegments());
      }
      return super.equals(other);
    }

    @Override
    public int hashCode() {
      return Objects.hash(segments);
    }

    @Override
    public int compareTo(OrderByFieldPath other) {
      // Inspired by com.google.cloud.firestore.FieldPath.
      int length = Math.min(this.getSegments().size(), other.getSegments().size());
      for (int i = 0; i < length; i++) {
        byte[] thisField = this.getSegments().get(i).getBytes(StandardCharsets.UTF_8);
        byte[] otherField = other.getSegments().get(i).getBytes(StandardCharsets.UTF_8);
        int cmp = UnsignedBytes.lexicographicalComparator().compare(thisField, otherField);
        if (cmp != 0) {
          return cmp;
        }
      }
      return Integer.compare(this.getSegments().size(), other.getSegments().size());
    }

    private static String unescapeFieldName(String fieldName) {
      if (fieldName.isEmpty()) {
        throw new IllegalArgumentException("quoted identifier cannot be empty");
      }
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < fieldName.length(); i++) {
        char c = fieldName.charAt(i);
        // Roughly speaking, there are 4 cases we care about:
        //   - carriage returns: \r and \r\n
        //   - unescaped quotes: `
        //   - non-escape sequences
        //   - escape sequences
        if (c == '`') {
          throw new IllegalArgumentException("quoted identifier cannot contain unescaped quote");
        } else if (c == '\r') {
          buf.append('\n');
          // Convert '\r\n' into '\n'
          if (i + 1 < fieldName.length() && fieldName.charAt(i + 1) == '\n') {
            i++;
          }
        } else if (c != '\\') {
          buf.append(c);
        } else if (i + 1 >= fieldName.length()) {
          throw new IllegalArgumentException("illegal trailing backslash");
        } else {
          i++;
          switch (fieldName.charAt(i)) {
            case 'a':
              buf.appendCodePoint(Ascii.BEL); // "Alert" control character
              break;
            case 'b':
              buf.append('\b');
              break;
            case 'f':
              buf.append('\f');
              break;
            case 'n':
              buf.append('\n');
              break;
            case 'r':
              buf.append('\r');
              break;
            case 't':
              buf.append('\t');
              break;
            case 'v':
              buf.appendCodePoint(Ascii.VT); // vertical tab
              break;
            case '?':
              buf.append('?'); // artifact of ancient C grammar
              break;
            case '\\':
              buf.append('\\');
              break;
            case '\'':
              buf.append('\'');
              break;
            case '"':
              buf.append('\"');
              break;
            case '`':
              buf.append('`');
              break;
            case '0':
            case '1':
            case '2':
            case '3':
              if (i + 3 > fieldName.length()) {
                throw new IllegalArgumentException("illegal octal escape sequence");
              }
              buf.appendCodePoint(unescapeOctal(fieldName.substring(i, i + 3)));
              i += 3;
              break;
            case 'x':
            case 'X':
              i++;
              if (i + 2 > fieldName.length()) {
                throw new IllegalArgumentException("illegal hex escape sequence");
              }
              buf.appendCodePoint(unescapeHex(fieldName.substring(i, i + 2)));
              i += 2;
              break;
            case 'u':
              i++;
              if (i + 4 > fieldName.length()) {
                throw new IllegalArgumentException("illegal unicode escape sequence");
              }
              buf.appendCodePoint(unescapeHex(fieldName.substring(i, i + 4)));
              i += 4;
              break;
            case 'U':
              i++;
              if (i + 8 > fieldName.length()) {
                throw new IllegalArgumentException("illegal unicode escape sequence");
              }
              buf.appendCodePoint(unescapeHex(fieldName.substring(i, i + 8)));
              i += 8;
              break;
            default:
              throw new IllegalArgumentException("illegal escape");
          }
        }
      }
      return buf.toString();
    }

    private static int unescapeOctal(String str) {
      int ch = 0;
      for (int i = 0; i < str.length(); i++) {
        ch = 8 * ch + octalValue(str.charAt(i));
      }
      if (!Character.isValidCodePoint(ch)) {
        throw new IllegalArgumentException("illegal codepoint");
      }
      return ch;
    }

    private static int unescapeHex(String str) {
      int ch = 0;
      for (int i = 0; i < str.length(); i++) {
        ch = 16 * ch + hexValue(str.charAt(i));
      }
      if (!Character.isValidCodePoint(ch)) {
        throw new IllegalArgumentException("illegal codepoint");
      }
      return ch;
    }

    private static int octalValue(char d) {
      if (d >= '0' && d <= '7') {
        return d - '0';
      } else {
        throw new IllegalArgumentException("illegal octal digit");
      }
    }

    private static int hexValue(char d) {
      if (d >= '0' && d <= '9') {
        return d - '0';
      } else if (d >= 'a' && d <= 'f') {
        return 10 + d - 'a';
      } else if (d >= 'A' && d <= 'F') {
        return 10 + d - 'A';
      } else {
        throw new IllegalArgumentException("illegal hex digit");
      }
    }
  }
}
