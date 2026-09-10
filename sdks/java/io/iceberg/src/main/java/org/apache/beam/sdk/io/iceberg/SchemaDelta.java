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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * What {@code unionByNameWith(fileSchema)} would change on a table, without changing it. Computed
 * by diffing the union result against the table schema by field id: existing fields keep their ids
 * and additions get fresh ones, so the diff is exact and independent of column order.
 *
 * <p>The union ignores table columns absent from the file, but every row of such a file reads null
 * in them, so a required column absent from the file is also a relaxation. The commit side stages
 * those explicitly via {@link #absentRequiredPaths()}.
 */
final class SchemaDelta {

  enum Kind {
    FIELD_ADDITION(SchemaEvolutionOption.ALLOW_FIELD_ADDITION),
    FIELD_RELAXATION(SchemaEvolutionOption.ALLOW_FIELD_RELAXATION),
    TYPE_PROMOTION(SchemaEvolutionOption.ALLOW_TYPE_PROMOTION),
    /** The union is impossible (for example string vs int); never allowed. */
    CONFLICT(null);

    final @Nullable SchemaEvolutionOption option;

    Kind(@Nullable SchemaEvolutionOption option) {
      this.option = option;
    }

    boolean allowedBy(SchemaEvolutionConfig config) {
      return option != null && config.allows(option);
    }
  }

  static final class Change {
    final Kind kind;

    /** Unquoted column path for the config lookup; empty for conflicts without a field. */
    final String path;

    final String description;

    /** A relaxation because the column is absent from the file, not declared optional. */
    final boolean absent;

    Change(Kind kind, String path, String description) {
      this(kind, path, description, false);
    }

    Change(Kind kind, String path, String description, boolean absent) {
      this.kind = kind;
      this.path = path;
      this.description = description;
      this.absent = absent;
    }

    boolean allowedBy(SchemaEvolutionConfig config, Pins pins) {
      // A pin also forbids relaxing the structs above it: a null ancestor nulls the pinned leaf.
      if (kind == Kind.FIELD_RELAXATION
          && (pins.isPinned(path) || pins.pinnedColumnBeneath(path) != null)) {
        return false;
      }
      return kind.allowedBy(config);
    }

    String disallowedReason(Pins pins) {
      if (kind == Kind.FIELD_RELAXATION) {
        if (pins.isPinned(path)) {
          return description + " (pinned as required)";
        }
        @Nullable String pin = pins.pinnedColumnBeneath(path);
        if (pin != null) {
          return description + " (ancestor of pinned column " + pin + ")";
        }
      }
      return description + " (needs " + kind.option + ")";
    }
  }

  private final List<Change> changes;

  private SchemaDelta(List<Change> changes) {
    this.changes = Collections.unmodifiableList(changes);
  }

  /**
   * What registering a file with {@code fileSchema} would need from the table, as changes ordered
   * by column path; the table itself is never modified. File column names no table can absorb
   * (dotted, empty, case-colliding) come back as conflicts without attempting the union.
   */
  static SchemaDelta classify(Table table, Schema fileSchema) {
    Schema before = table.schema();
    if (before.sameSchema(fileSchema)) {
      return new SchemaDelta(Collections.emptyList());
    }

    List<Change> nameConflicts = new ArrayList<>();
    findInvalidNames(fileSchema.asStruct(), "", nameConflicts);
    findCaseCollisions(before.asStruct(), fileSchema.asStruct(), "", nameConflicts);
    if (!nameConflicts.isEmpty()) {
      return new SchemaDelta(nameConflicts);
    }

    List<Change> absent = new ArrayList<>();
    findAbsentRequired(before.asStruct(), fileSchema.asStruct(), "", absent);
    Schema merged;
    try {
      // The absent-path relaxations are applied here too, so anything Iceberg refuses (an
      // identifier field, say) is classified as this file's conflict instead of surfacing
      // mid-transaction under a cross-schema message.
      UpdateSchema update = table.updateSchema().unionByNameWith(fileSchema);
      for (Change change : absent) {
        update = update.makeColumnOptional(change.path);
      }
      merged = update.apply();
    } catch (ValidationException | IllegalArgumentException e) {
      // SchemaUpdate reports type conflicts through both exception types
      return conflict(e.getClass().getSimpleName() + ": " + AddFiles.errorMessage(e));
    }
    Map<String, Change> absentByPath = new HashMap<>();
    for (Change change : absent) {
      absentByPath.put(change.path, change);
    }
    return diff(before, merged, absentByPath);
  }

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
  @VisibleForTesting
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
                    + quoteIfDotted(field.name())
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
                    + quoteIfDotted(seen)
                    + " and "
                    + prefix
                    + quoteIfDotted(field.name())
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
  @VisibleForTesting
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
                        + quoteIfDotted(fileField.name())
                        + " differs only in case from table column "
                        + quoteIfDotted(tableField.name())
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

  /**
   * Required table columns with no counterpart in the file, by name per level. Children are checked
   * only when their parent is present; an absent struct is the relaxation itself. Descends through
   * list elements and map values (paths use {@code element} and {@code value}, which
   * makeColumnOptional accepts); map keys are required by definition. FileSchemas tightening stops
   * at lists and maps for a different reason (ambiguous null counts); the two are independent.
   */
  private static void findAbsentRequired(
      Types.StructType tableStruct,
      Types.StructType fileStruct,
      String prefix,
      List<Change> changes) {
    for (Types.NestedField field : tableStruct.fields()) {
      String rawPath = prefix + field.name();
      Types.NestedField fileField = fileStruct.field(field.name());
      if (fileField == null) {
        if (field.isRequired()) {
          changes.add(
              new Change(
                  Kind.FIELD_RELAXATION,
                  rawPath,
                  "relax "
                      + prefix
                      + quoteIfDotted(field.name())
                      + " to optional (absent from file)",
                  true));
        }
        continue;
      }
      findAbsentRequiredInType(field.type(), fileField.type(), rawPath, changes);
    }
  }

  private static void findAbsentRequiredInType(
      Type tableType, Type fileType, String rawPath, List<Change> changes) {
    if (tableType.isStructType() && fileType.isStructType()) {
      findAbsentRequired(tableType.asStructType(), fileType.asStructType(), rawPath + ".", changes);
    } else if (tableType.isListType() && fileType.isListType()) {
      findAbsentRequiredInType(
          tableType.asListType().elementType(),
          fileType.asListType().elementType(),
          rawPath + ".element",
          changes);
    } else if (tableType.isMapType() && fileType.isMapType()) {
      findAbsentRequiredInType(
          tableType.asMapType().valueType(),
          fileType.asMapType().valueType(),
          rawPath + ".value",
          changes);
    }
  }

  /** Paths of required table columns absent from the file; the union alone does not relax them. */
  List<String> absentRequiredPaths() {
    List<String> paths = new ArrayList<>();
    for (Change change : changes) {
      if (change.absent) {
        paths.add(change.path);
      }
    }
    return paths;
  }

  private static SchemaDelta conflict(String message) {
    List<Change> changes = new ArrayList<>();
    changes.add(new Change(Kind.CONFLICT, "", message));
    return new SchemaDelta(changes);
  }

  /**
   * Changes from {@code before} to {@code after}, ordered by field path. Fields are matched by id;
   * paths only appear in messages (quoted when a name contains a dot). Anything a union by name
   * cannot produce is reported as a conflict so it is never applied unclassified.
   */
  static SchemaDelta diff(Schema before, Schema after) {
    return diff(before, after, Collections.emptyMap());
  }

  /**
   * {@code absentByPath}: classify's absent-column relaxations, emitted here in path order where
   * the diff sees the required-to-optional flip that classify itself staged.
   */
  private static SchemaDelta diff(Schema before, Schema after, Map<String, Change> absentByPath) {
    Map<String, Change> absentRemaining = new HashMap<>(absentByPath);
    Map<Integer, Types.NestedField> beforeById = TypeUtil.indexById(before.asStruct());
    Map<Integer, Types.NestedField> afterById = TypeUtil.indexById(after.asStruct());
    Map<Integer, Integer> parentById = TypeUtil.indexParents(after.asStruct());
    Map<Integer, String> rawPathById = TypeUtil.indexNameById(after.asStruct());
    Map<Integer, String> pathById =
        TypeUtil.indexQuotedNameById(after.asStruct(), SchemaDelta::quoteIfDotted);

    List<Integer> idsByPath = new ArrayList<>(afterById.keySet());
    idsByPath.sort(
        (a, b) ->
            checkStateNotNull(rawPathById.get(a)).compareTo(checkStateNotNull(rawPathById.get(b))));

    List<Change> changes = new ArrayList<>();
    for (Integer id : idsByPath) {
      String path = checkStateNotNull(pathById.get(id));
      String rawPath = checkStateNotNull(rawPathById.get(id));
      Types.NestedField newField = checkStateNotNull(afterById.get(id));
      Types.NestedField oldField = beforeById.get(id);
      if (oldField == null) {
        if (!hasAddedAncestor(id, parentById, beforeById)) {
          changes.add(
              new Change(
                  Kind.FIELD_ADDITION,
                  rawPath,
                  "add " + optionality(newField) + " " + path + " " + describe(newField.type())));
        }
        continue;
      }
      compareField(path, rawPath, oldField, newField, absentRemaining, changes);
    }
    checkState(
        absentRemaining.isEmpty(),
        "absent-column relaxations did not surface in the diff: %s",
        absentRemaining.keySet());

    Map<Integer, String> beforePathById =
        TypeUtil.indexQuotedNameById(before.asStruct(), SchemaDelta::quoteIfDotted);
    List<String> removed = new ArrayList<>();
    for (Integer id : beforeById.keySet()) {
      if (!afterById.containsKey(id)) {
        removed.add(checkStateNotNull(beforePathById.get(id)));
      }
    }
    Collections.sort(removed);
    for (String path : removed) {
      changes.add(new Change(Kind.CONFLICT, "", "field removed: " + path));
    }
    return new SchemaDelta(changes);
  }

  /**
   * Attribute by attribute: name, doc and defaults must be equal; required to optional is the
   * relaxation; primitive types must be equal or a promotion; nested types must stay the same kind,
   * their children are compared on their own ids.
   */
  private static void compareField(
      String path,
      String rawPath,
      Types.NestedField oldField,
      Types.NestedField newField,
      Map<String, Change> absentRemaining,
      List<Change> changes) {
    if (!oldField.name().equals(newField.name())) {
      changes.add(
          new Change(
              Kind.CONFLICT,
              rawPath,
              "renamed " + path + " from " + oldField.name() + " to " + newField.name()));
    }
    if (!Objects.equals(oldField.doc(), newField.doc())) {
      // benign but unsupported: schema evolution has no option for doc updates
      changes.add(
          new Change(Kind.CONFLICT, rawPath, "doc changed on " + path + " (not supported)"));
    }
    if (!Objects.equals(oldField.initialDefault(), newField.initialDefault())
        || !Objects.equals(oldField.writeDefault(), newField.writeDefault())) {
      changes.add(
          new Change(Kind.CONFLICT, rawPath, "default changed on " + path + " (not supported)"));
    }
    if (oldField.isRequired() && newField.isOptional()) {
      @Nullable Change absent = absentRemaining.remove(rawPath);
      changes.add(
          absent != null
              ? absent
              : new Change(Kind.FIELD_RELAXATION, rawPath, "relax " + path + " to optional"));
    } else if (oldField.isOptional() && newField.isRequired()) {
      changes.add(new Change(Kind.CONFLICT, rawPath, "optionality tightened on " + path));
    }
    boolean oldPrimitive = oldField.type().isPrimitiveType();
    boolean newPrimitive = newField.type().isPrimitiveType();
    if (oldPrimitive && newPrimitive) {
      if (oldField.type().equals(newField.type())) {
        return;
      }
      if (TypeUtil.isPromotionAllowed(oldField.type(), newField.type().asPrimitiveType())) {
        changes.add(
            new Change(
                Kind.TYPE_PROMOTION,
                rawPath,
                "promote " + path + " " + oldField.type() + " to " + newField.type()));
      } else {
        changes.add(
            new Change(
                Kind.CONFLICT,
                rawPath,
                "type changed on "
                    + path
                    + " from "
                    + oldField.type()
                    + " to "
                    + newField.type()
                    + " (not a promotion)"));
      }
    } else if (oldPrimitive != newPrimitive
        || oldField.type().typeId() != newField.type().typeId()) {
      changes.add(
          new Change(
              Kind.CONFLICT,
              rawPath,
              "type changed on "
                  + path
                  + " from "
                  + describe(oldField.type())
                  + " to "
                  + describe(newField.type())));
    }
  }

  /** Renders a type without field ids: file-side ids are positional and would only mislead. */
  private static String describe(Type type) {
    if (type.isStructType()) {
      StringBuilder rendered = new StringBuilder("struct<");
      List<Types.NestedField> fields = type.asStructType().fields();
      for (int i = 0; i < fields.size(); i++) {
        Types.NestedField field = fields.get(i);
        if (i > 0) {
          rendered.append(", ");
        }
        rendered
            .append(quoteIfDotted(field.name()))
            .append(": ")
            .append(optionality(field))
            .append(" ")
            .append(describe(field.type()));
      }
      return rendered.append(">").toString();
    }
    if (type.isListType()) {
      return "list<" + describe(type.asListType().elementType()) + ">";
    }
    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      return "map<" + describe(map.keyType()) + ", " + describe(map.valueType()) + ">";
    }
    return type.toString();
  }

  /** A field added inside a newly added struct is reported once, as part of its ancestor. */
  private static boolean hasAddedAncestor(
      int id, Map<Integer, Integer> parentById, Map<Integer, Types.NestedField> beforeById) {
    Integer parent = parentById.get(id);
    while (parent != null) {
      if (!beforeById.containsKey(parent)) {
        return true;
      }
      parent = parentById.get(parent);
    }
    return false;
  }

  private static String quoteIfDotted(String name) {
    if (name.contains(".")) {
      return "`" + name + "`";
    }
    return name;
  }

  private static String optionality(Types.NestedField field) {
    return field.isOptional() ? "optional" : "required";
  }

  boolean isEmpty() {
    return changes.isEmpty();
  }

  Set<Kind> kinds() {
    Set<Kind> kinds = EnumSet.noneOf(Kind.class);
    for (Change change : changes) {
      kinds.add(change.kind);
    }
    return kinds;
  }

  List<String> descriptions() {
    List<String> descriptions = new ArrayList<>();
    for (Change change : changes) {
      descriptions.add(change.description);
    }
    return Collections.unmodifiableList(descriptions);
  }

  @Nullable String conflict() {
    for (Change change : changes) {
      if (change.kind == Kind.CONFLICT) {
        return change.description;
      }
    }
    return null;
  }

  boolean allowedBy(SchemaEvolutionConfig config) {
    Pins pins = new Pins(config.getRequiredColumns());
    for (Change change : changes) {
      if (!change.allowedBy(config, pins)) {
        return false;
      }
    }
    return true;
  }

  /** Why {@link #allowedBy} is false; empty when it is true. */
  String disallowedReason(SchemaEvolutionConfig config) {
    List<String> conflicts = new ArrayList<>();
    for (Change change : changes) {
      if (change.kind == Kind.CONFLICT) {
        conflicts.add(change.description);
      }
    }
    if (!conflicts.isEmpty()) {
      return "file schema conflicts with the table schema: " + String.join("; ", conflicts);
    }
    Pins pins = new Pins(config.getRequiredColumns());
    List<String> disallowed = new ArrayList<>();
    for (Change change : changes) {
      if (!change.allowedBy(config, pins)) {
        disallowed.add(change.disallowedReason(pins));
      }
    }
    if (disallowed.isEmpty()) {
      return "";
    }
    return "file schema needs changes that are not allowed: " + String.join("; ", disallowed);
  }

  @Override
  public String toString() {
    return "SchemaDelta" + descriptions();
  }
}
