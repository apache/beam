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

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

class SortOrderUtils {
  private static final Pattern MODIFIERS =
      Pattern.compile(
          "^(?<dir>asc|desc)?\\s*(nulls\\s+(?<nulls>first|last))?\\s*$", Pattern.CASE_INSENSITIVE);

  static SortOrder toSortOrder(
      @Nullable List<String> fields, org.apache.beam.sdk.schemas.Schema beamSchema) {
    return toSortOrder(fields, IcebergUtils.beamSchemaToIcebergSchema(beamSchema));
  }

  static SortOrder toSortOrder(@Nullable List<String> fields, Schema schema) {
    if (fields == null || fields.isEmpty()) {
      return SortOrder.unsorted();
    }
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (String field : fields) {
      ParsedSortField parsed = parse(field);
      if (parsed.ascending) {
        builder.asc(parsed.term, parsed.nullOrder);
      } else {
        builder.desc(parsed.term, parsed.nullOrder);
      }
    }
    return builder.build();
  }

  private static ParsedSortField parse(String field) {
    int splitAt = findTopLevelWhitespace(field);
    String termStr = (splitAt < 0 ? field : field.substring(0, splitAt)).trim();
    String rest = (splitAt < 0 ? "" : field.substring(splitAt)).trim();

    Term term = PartitionUtils.toIcebergTerm(termStr);
    boolean ascending = true;
    @Nullable NullOrder nullOrder = null;

    if (!rest.isEmpty()) {
      Matcher matcher = MODIFIERS.matcher(rest);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            "Could not parse sort modifiers '"
                + rest
                + "' in '"
                + field
                + "'. Expected: [asc|desc] [nulls first|nulls last].");
      }
      String dir = matcher.group("dir");
      if (dir != null) {
        ascending = dir.toLowerCase(Locale.ROOT).equals("asc");
      }
      String nulls = matcher.group("nulls");
      if (nulls != null) {
        nullOrder =
            nulls.toLowerCase(Locale.ROOT).equals("first")
                ? NullOrder.NULLS_FIRST
                : NullOrder.NULLS_LAST;
      }
    }

    if (nullOrder == null) {
      nullOrder = ascending ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
    }
    return new ParsedSortField(term, ascending, nullOrder);
  }

  private static int findTopLevelWhitespace(String s) {
    int depth = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && Character.isWhitespace(c)) {
        return i;
      }
    }
    return -1;
  }

  private static class ParsedSortField {
    final Term term;
    final boolean ascending;
    final NullOrder nullOrder;

    ParsedSortField(Term term, boolean ascending, NullOrder nullOrder) {
      this.term = term;
      this.ascending = ascending;
      this.nullOrder = nullOrder;
    }
  }
}
