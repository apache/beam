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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import org.apache.beam.sdk.io.iceberg.SchemaChange.Kind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaDeltaTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestName testName = new TestName();

  private static final Schema TABLE =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "score", Types.FloatType.get()),
          optional(
              4,
              "address",
              Types.StructType.of(
                  required(5, "city", Types.StringType.get()),
                  optional(6, "zip", Types.IntegerType.get()))),
          optional(7, "tags", Types.ListType.ofOptional(8, Types.StringType.get())),
          optional(9, "amount", Types.DecimalType.of(9, 2)));

  private static final SchemaEvolutionConfig ALL =
      SchemaEvolutionConfig.of(SchemaEvolutionOption.values());

  private Table table;
  private Schema tableCreatedWith;

  private SchemaDelta classify(Schema fileSchema) {
    return classify(TABLE, fileSchema);
  }

  private SchemaDelta classify(Schema tableSchema, Schema fileSchema) {
    if (table == null) {
      table =
          warehouse.createTable(
              TableIdentifier.of("default", testName.getMethodName()), tableSchema);
      tableCreatedWith = tableSchema;
    } else if (tableCreatedWith != null) {
      assertTrue(
          "classify already created the table with a different schema",
          tableCreatedWith.sameSchema(tableSchema));
    }
    return SchemaDelta.classify(table, fileSchema);
  }

  private static SchemaEvolutionConfig pinned(String... columns) {
    return SchemaEvolutionConfig.builder()
        .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
        .setRequiredColumns(new HashSet<>(Arrays.asList(columns)))
        .build();
  }

  // ---- empty deltas

  @Test
  public void testIdenticalSchemaIsEmpty() {
    assertTrue(classify(TABLE).isEmpty());
    assertTrue(classify(TABLE).allowedBy(SchemaEvolutionConfig.disabled()));
    // The catalog renumbered TABLE's nested ids, so the calls above walk the full diff; only a
    // schema with the table's own ids takes the sameSchema fast path.
    Table created = checkStateNotNull(table);
    assertTrue(SchemaDelta.classify(created, created.schema()).isEmpty());
  }

  // ---- required columns absent from the file

  @Test
  public void testAbsentRequiredColumnIsRelaxation() {
    Schema file = new Schema(optional(1, "name", Types.StringType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(EnumSet.of(Kind.FIELD_RELAXATION), delta.kinds());
    assertEquals(Arrays.asList("relax id to optional (absent from file)"), delta.descriptions());
    assertEquals(Arrays.asList("id"), delta.absentRequiredPaths());
    assertTrue(
        delta.allowedBy(SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_RELAXATION)));
    assertFalse(
        delta.allowedBy(SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION)));
    assertFalse(delta.allowedBy(pinned("id")));
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax id to optional (absent from file) (pinned as required)",
        delta.disallowedReason(pinned("id")));
  }

  @Test
  public void testAbsentNestedRequiredChildIsRelaxation() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2, "address", Types.StructType.of(optional(3, "zip", Types.IntegerType.get()))));
    SchemaDelta delta = classify(file);
    assertEquals(
        Arrays.asList("relax address.city to optional (absent from file)"), delta.descriptions());
    assertEquals(Arrays.asList("address.city"), delta.absentRequiredPaths());
  }

  @Test
  public void testAbsentRequiredStructIsOneRelaxation() {
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2, "address", Types.StructType.of(required(3, "city", Types.StringType.get()))));
    Schema file = new Schema(required(1, "id", Types.LongType.get()));
    SchemaDelta delta = classify(tableSchema, file);
    assertEquals(
        Arrays.asList("relax address to optional (absent from file)"), delta.descriptions());
  }

  /** Pins, absent-path reporting and makeColumnOptional share the element/value path spelling. */
  @Test
  public void testAbsentRequiredUnderListAndMapIsRelaxation() {
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "items",
                Types.ListType.ofOptional(
                    3, Types.StructType.of(required(4, "sku", Types.StringType.get())))),
            optional(
                5,
                "attrs",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(required(8, "v", Types.StringType.get())))));
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "items",
                Types.ListType.ofOptional(
                    3, Types.StructType.of(optional(4, "qty", Types.IntegerType.get())))),
            optional(
                5,
                "attrs",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(optional(8, "w", Types.StringType.get())))));
    SchemaDelta delta = classify(tableSchema, file);
    assertEquals(
        Arrays.asList(
            "relax attrs.value.v to optional (absent from file)",
            "add optional attrs.value.w string",
            "add optional items.element.qty int",
            "relax items.element.sku to optional (absent from file)"),
        delta.descriptions());
    assertEquals(Arrays.asList("attrs.value.v", "items.element.sku"), delta.absentRequiredPaths());
    assertFalse(delta.allowedBy(pinned("items.element.sku")));
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax items.element.sku to optional (absent from file) (pinned as required)",
        delta.disallowedReason(pinned("items.element.sku")));
  }

  @Test
  public void testMultipleAbsentRequiredColumns() {
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "name", Types.StringType.get()));
    Schema file = new Schema(optional(1, "name", Types.StringType.get()));
    SchemaDelta delta = classify(tableSchema, file);
    assertEquals(
        Arrays.asList(
            "relax id to optional (absent from file)",
            "relax region to optional (absent from file)"),
        delta.descriptions());
    assertEquals(Arrays.asList("id", "region"), delta.absentRequiredPaths());
  }

  /**
   * Pins classify's own staging of makeColumnOptional inside the try: Iceberg's identifier-field
   * refusal must come out classified as this file's conflict, not thrown mid-transaction.
   */
  @Test
  public void testAbsentRequiredIdentifierFieldIsConflict() {
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));
    table =
        warehouse.createTable(TableIdentifier.of("default", testName.getMethodName()), tableSchema);
    table.updateSchema().setIdentifierFields("id").commit();
    Schema file = new Schema(optional(1, "name", Types.StringType.get()));
    SchemaDelta delta = SchemaDelta.classify(table, file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    assertNotNull(delta.conflict());
  }

  // ---- additions

  /** A required new column is still added optional (addColumn always adds optional). */
  @Test
  public void testTopLevelAddition() {
    Schema file =
        new Schema(
            required(1, "email", Types.StringType.get()), required(2, "id", Types.LongType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(EnumSet.of(Kind.FIELD_ADDITION), delta.kinds());
    assertEquals(Arrays.asList("add optional email string"), delta.descriptions());
    // classify's apply() never commits: the table is untouched.
    assertNull(checkStateNotNull(table).schema().findField("email"));
  }

  @Test
  public void testNestedAddition() {
    Schema file =
        new Schema(
            required(3, "id", Types.LongType.get()),
            optional(
                1,
                "address",
                Types.StructType.of(
                    required(4, "city", Types.StringType.get()),
                    optional(2, "country", Types.StringType.get()))));
    SchemaDelta delta = classify(file);
    assertEquals(EnumSet.of(Kind.FIELD_ADDITION), delta.kinds());
    assertEquals(Arrays.asList("add optional address.country string"), delta.descriptions());
  }

  @Test
  public void testAddedStructIsReportedOnce() {
    Schema file =
        new Schema(
            required(9, "id", Types.LongType.get()),
            optional(
                1,
                "geo",
                Types.StructType.of(
                    optional(2, "lat", Types.DoubleType.get()),
                    optional(3, "lon", Types.DoubleType.get()))));
    SchemaDelta delta = classify(file);
    assertEquals(
        Arrays.asList("add optional geo struct<lat: optional double, lon: optional double>"),
        delta.descriptions());
  }

  // ---- relaxations and pins

  @Test
  public void testRelaxations() {
    Schema file =
        new Schema(
            optional(1, "id", Types.LongType.get()),
            optional(
                2, "address", Types.StructType.of(optional(3, "city", Types.StringType.get()))));
    SchemaDelta delta = classify(file);
    assertEquals(EnumSet.of(Kind.FIELD_RELAXATION), delta.kinds());
    assertEquals(
        Arrays.asList("relax address.city to optional", "relax id to optional"),
        delta.descriptions());
    assertTrue(delta.allowedBy(ALL));
    assertFalse(delta.allowedBy(pinned("address.city")));
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax address.city to optional (pinned as required)",
        delta.disallowedReason(pinned("address.city")));
  }

  @Test
  public void testRelaxingTheAncestorOfAPinnedColumnIsRefused() {
    Schema tableSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2, "address", Types.StructType.of(required(3, "city", Types.StringType.get()))));
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2, "address", Types.StructType.of(required(3, "city", Types.StringType.get()))));
    SchemaDelta delta = classify(tableSchema, file);
    assertFalse(delta.allowedBy(pinned("address.city")));
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax address to optional (ancestor of pinned column address.city)",
        delta.disallowedReason(pinned("address.city")));
    // When several pins forbid the same relaxation, the lexicographically first is reported,
    // independent of the set's iteration order.
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax address to optional (ancestor of pinned column address.city)",
        delta.disallowedReason(pinned("address.zip", "address.city")));
  }

  // ---- promotion

  @Test
  public void testPromotions() {
    Schema file =
        new Schema(
            required(9, "id", Types.LongType.get()),
            optional(1, "score", Types.DoubleType.get()),
            optional(2, "amount", Types.DecimalType.of(18, 2)),
            optional(
                3,
                "address",
                Types.StructType.of(
                    required(5, "city", Types.StringType.get()),
                    optional(4, "zip", Types.LongType.get()))));
    SchemaDelta delta = classify(file);
    assertEquals(EnumSet.of(Kind.TYPE_PROMOTION), delta.kinds());
    assertEquals(
        Arrays.asList(
            "promote address.zip int to long",
            "promote amount decimal(9, 2) to decimal(18, 2)",
            "promote score float to double"),
        delta.descriptions());
  }

  // ---- combined and gating

  @Test
  public void testCombinedDeltaReportsEveryKind() {
    Schema file =
        new Schema(
            optional(1, "id", Types.LongType.get()),
            optional(2, "score", Types.DoubleType.get()),
            optional(3, "email", Types.StringType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(
        EnumSet.of(Kind.FIELD_ADDITION, Kind.FIELD_RELAXATION, Kind.TYPE_PROMOTION), delta.kinds());
    assertNull(delta.conflict());
    assertTrue(delta.allowedBy(ALL));
    assertFalse(
        delta.allowedBy(
            SchemaEvolutionConfig.of(
                SchemaEvolutionOption.ALLOW_FIELD_ADDITION,
                SchemaEvolutionOption.ALLOW_TYPE_PROMOTION)));
    assertEquals(
        "file schema needs changes that are not allowed: "
            + "relax id to optional (needs ALLOW_FIELD_RELAXATION)",
        delta.disallowedReason(
            SchemaEvolutionConfig.of(
                SchemaEvolutionOption.ALLOW_FIELD_ADDITION,
                SchemaEvolutionOption.ALLOW_TYPE_PROMOTION)));
    assertEquals("", delta.disallowedReason(ALL));
  }

  // ---- names the table cannot absorb: one classify wiring test per check; exhaustive
  //      shapes are covered directly on the walks below

  @Test
  public void testDottedColumnNameIsConflict() {
    Schema file = new Schema(optional(1, "address.zip", Types.IntegerType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.contains("path separator"));
  }

  /** The union rejects an empty name only at the top level; nested ones would be added. */
  @Test
  public void testEmptyColumnNameIsConflict() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "", Types.StringType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.contains("empty column name"));
  }

  @Test
  public void testCaseOnlyDifferenceFromTableIsConflict() {
    Schema file =
        new Schema(
            optional(1, "NAME", Types.StringType.get()), required(2, "id", Types.LongType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.contains("differs only in case from table column name"));
  }

  /** Two new columns differing only in case would break the lower-case index between them. */
  @Test
  public void testFileInternalCaseCollisionIsConflict() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "email", Types.StringType.get()),
            optional(3, "EMAIL", Types.StringType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.contains("email and EMAIL differ only in case"));
  }

  // ---- Iceberg behaviors classify depends on; after a version bump failure, start here

  /** The union never tightens: a reordered, subset, stricter-optionality file changes nothing. */
  @Test
  public void testReorderedSubsetTighterFileIsCovered() {
    Schema file =
        new Schema(
            required(1, "name", Types.StringType.get()), required(2, "id", Types.LongType.get()));
    assertTrue(classify(file).isEmpty());
  }

  /**
   * Iceberg 1.11's union ignores a file primitive that promotes to the table's type
   * (UnionByNameVisitor.isIgnorableTypeUpdate): readers widen narrower files on read.
   */
  @Test
  public void testNarrowerFileTypeIsCovered() {
    Schema file = new Schema(required(1, "id", Types.IntegerType.get()));
    assertTrue(classify(file).isEmpty());
  }

  /** The union throws for an impossible type change; classify catches and classifies it. */
  @Test
  public void testTypeMismatchIsConflict() {
    Schema file = new Schema(optional(1, "name", Types.IntegerType.get()));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    assertNotNull(delta.conflict());
    assertFalse(delta.allowedBy(ALL));
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.startsWith("file schema conflicts with the table schema: "));
  }

  /**
   * Unreachable through AddFiles today (Parquet conversion never sets docs), pinned as deliberate:
   * the union would silently rewrite the table's doc, so a doc-bearing file schema must conflict.
   */
  @Test
  public void testDocBearingFileSchemaIsConflict() {
    Schema file = new Schema(required(1, "id", Types.LongType.get(), "the id"));
    SchemaDelta delta = classify(file);
    assertEquals(delta.toString(), EnumSet.of(Kind.CONFLICT), delta.kinds());
    String reason = delta.disallowedReason(ALL);
    assertTrue(reason, reason.contains("doc changed on id"));
  }

  // ---- diff sanity checks, driven directly

  /**
   * No classify input reaches these branches (a union never removes, tightens, renames, narrows or
   * edits defaults; it throws first), but the "never applied unclassified" contract says diff must
   * flag them if Iceberg ever changes.
   */
  @Test
  public void testDiffFlagsChangesTheUnionCannotProduce() {
    Schema id = new Schema(required(1, "id", Types.LongType.get()));

    Schema withStruct =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    optional(3, "x", Types.IntegerType.get()),
                    optional(4, "y", Types.IntegerType.get()))));
    SchemaDelta removed = SchemaDelta.diff(withStruct, id);
    assertFalse(removed.allowedBy(ALL));
    assertEquals(
        "file schema conflicts with the table schema: "
            + "field removed: s; field removed: s.x; field removed: s.y",
        removed.disallowedReason(ALL));

    Schema optionalA =
        new Schema(
            optional(1, "a", Types.StructType.of(optional(2, "b", Types.IntegerType.get()))));
    Schema requiredA =
        new Schema(
            required(1, "a", Types.StructType.of(optional(2, "b", Types.IntegerType.get()))));
    assertEquals(
        Arrays.asList("optionality tightened on a"),
        SchemaDelta.diff(optionalA, requiredA).descriptions());

    Schema renamed = new Schema(required(1, "id2", Types.LongType.get()));
    assertEquals(
        Arrays.asList("renamed id2 from id to id2"), SchemaDelta.diff(id, renamed).descriptions());

    Schema narrowed = new Schema(required(1, "id", Types.IntegerType.get()));
    SchemaDelta narrowing = SchemaDelta.diff(id, narrowed);
    assertEquals(
        Arrays.asList("type changed on id from long to int (not a promotion)"),
        narrowing.descriptions());
    assertEquals(EnumSet.of(Kind.CONFLICT), narrowing.kinds());

    Schema defaulted =
        new Schema(
            Types.NestedField.optional("id")
                .withId(1)
                .ofType(Types.LongType.get())
                .withWriteDefault(org.apache.iceberg.expressions.Literal.of(7L))
                .build());
    assertEquals(
        EnumSet.of(Kind.CONFLICT, Kind.FIELD_RELAXATION), SchemaDelta.diff(id, defaulted).kinds());

    Schema structOfX =
        new Schema(
            optional(1, "s", Types.StructType.of(optional(2, "x", Types.IntegerType.get()))));
    Schema primitiveS = new Schema(optional(1, "s", Types.StringType.get()));
    assertEquals(
        Arrays.asList(
            "type changed on s from struct<x: optional int> to string", "field removed: s.x"),
        SchemaDelta.diff(structOfX, primitiveS).descriptions());

    Schema withContainers =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "attrs",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.IntegerType.get())),
            optional(5, "tags", Types.ListType.ofOptional(6, Types.StringType.get())));
    assertEquals(
        Arrays.asList("add optional attrs map<string, int>", "add optional tags list<string>"),
        SchemaDelta.diff(id, withContainers).descriptions());
  }

  /** Iceberg rejects a schema where a dotted name equals a nested path, so only quoting matters. */
  @Test
  public void testDottedNameIsQuotedAndDoesNotSwallowSiblings() {
    Schema before = new Schema(required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "a.b", Types.StringType.get()),
            optional(3, "a", Types.StructType.of(optional(4, "c", Types.IntegerType.get()))));
    SchemaDelta delta = SchemaDelta.diff(before, after);
    assertEquals(
        Arrays.asList("add optional a struct<c: optional int>", "add optional `a.b` string"),
        delta.descriptions());
  }
}
