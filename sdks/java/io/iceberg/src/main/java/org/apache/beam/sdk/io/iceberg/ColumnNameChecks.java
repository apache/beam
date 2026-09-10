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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.SchemaDelta.Change;
import org.apache.beam.sdk.io.iceberg.SchemaDelta.Kind;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rejections of file column names no table can absorb, run by {@link SchemaDelta#classify} before
 * the union is attempted so the conflict is attributed to the offending file, with a message naming
 * the column.
 */
final class ColumnNameChecks {
  private ColumnNameChecks() {}

  /**
   * Adds a conflict for every file column name no table can absorb, at every level including
   * structs the table does not have yet: names containing a literal dot, empty names, and pairs of
   * names at one level differing only in case. A dot is a conflict because Iceberg's name APIs,
   * pins, aliases and ignores all treat it as a path separator, and a colliding struct in a later
   * window would make the whole table unresolvable by name; rejected whether or not it collides
   * today. An empty name would otherwise be added as a real column (the union only rejects it at
   * the top level). A case-only pair would be added as two columns, after which Iceberg cannot
   * build the lower-case name index.
   */
  static void findInvalidNames(Types.StructType struct, String prefix, List<Change> changes) {
    Map<String, String> seenByLowerCase = new HashMap<>();
    for (Types.NestedField field : struct.fields()) {
      String rawPath = prefix + field.name();
      if (field.name().isEmpty()) {
        String at = prefix.isEmpty() ? "" : " under " + prefix.substring(0, prefix.length() - 1);
        changes.add(new Change(Kind.CONFLICT, rawPath, "empty column name" + at));
      } else if (field.name().contains(".")) {
        changes.add(
            new Change(
                Kind.CONFLICT,
                rawPath,
                "column name "
                    + SchemaDelta.quoteIfDotted(field.name())
                    + " contains '.', which Iceberg treats as a path separator; rename the column"
                    + " at its source"));
      }
      @Nullable String seen =
          seenByLowerCase.put(field.name().toLowerCase(Locale.ROOT), field.name());
      if (seen != null) {
        changes.add(
            new Change(
                Kind.CONFLICT,
                rawPath,
                "columns "
                    + prefix
                    + SchemaDelta.quoteIfDotted(seen)
                    + " and "
                    + prefix
                    + SchemaDelta.quoteIfDotted(field.name())
                    + " differ only in case; rename one or map it with a column alias"));
      }
      findInvalidNamesInType(field.type(), rawPath, changes);
    }
  }

  private static void findInvalidNamesInType(Type type, String rawPath, List<Change> changes) {
    if (type.isStructType()) {
      findInvalidNames(type.asStructType(), rawPath + ".", changes);
    } else if (type.isListType()) {
      findInvalidNamesInType(type.asListType().elementType(), rawPath + ".element", changes);
    } else if (type.isMapType()) {
      findInvalidNamesInType(type.asMapType().valueType(), rawPath + ".value", changes);
    }
  }

  /**
   * Adds a conflict for every file column whose name matches a table column at the same level only
   * case-insensitively; exact matches and genuinely new names pass. Such a column would be added as
   * a separate column, after which Iceberg cannot build the lower-case name index and every
   * case-insensitive reader of the table fails.
   */
  static void findCaseCollisions(
      Types.StructType tableStruct,
      Types.StructType fileStruct,
      String prefix,
      List<Change> changes) {
    for (Types.NestedField fileField : fileStruct.fields()) {
      String rawPath = prefix + fileField.name();
      Types.NestedField exact = tableStruct.field(fileField.name());
      if (exact == null) {
        for (Types.NestedField tableField : tableStruct.fields()) {
          if (tableField.name().equalsIgnoreCase(fileField.name())) {
            changes.add(
                new Change(
                    Kind.CONFLICT,
                    rawPath,
                    "column "
                        + prefix
                        + SchemaDelta.quoteIfDotted(fileField.name())
                        + " differs only in case from table column "
                        + SchemaDelta.quoteIfDotted(tableField.name())
                        + "; rename it or map it with a column alias"));
            break;
          }
        }
        continue;
      }
      findCaseCollisionsInType(exact.type(), fileField.type(), rawPath, changes);
    }
  }

  private static void findCaseCollisionsInType(
      Type tableType, Type fileType, String rawPath, List<Change> changes) {
    if (tableType.isStructType() && fileType.isStructType()) {
      findCaseCollisions(tableType.asStructType(), fileType.asStructType(), rawPath + ".", changes);
    } else if (tableType.isListType() && fileType.isListType()) {
      findCaseCollisionsInType(
          tableType.asListType().elementType(),
          fileType.asListType().elementType(),
          rawPath + ".element",
          changes);
    } else if (tableType.isMapType() && fileType.isMapType()) {
      findCaseCollisionsInType(
          tableType.asMapType().valueType(),
          fileType.asMapType().valueType(),
          rawPath + ".value",
          changes);
    }
  }
}
