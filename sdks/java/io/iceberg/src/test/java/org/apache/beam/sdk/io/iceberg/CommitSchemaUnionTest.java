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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.iceberg.CommitSchemaUnion.Committer;
import org.apache.beam.sdk.io.iceberg.CommitSchemaUnion.IncompatibleSchemaException;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CommitSchemaUnionTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestName testName = new TestName();

  private static final Schema TABLE =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "score", Types.FloatType.get()),
          required(4, "region", Types.StringType.get()));

  private static final SchemaEvolutionConfig ALL =
      SchemaEvolutionConfig.of(SchemaEvolutionOption.values());
  private static final SchemaEvolutionConfig ADDITION_ONLY =
      SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION);

  private HadoopCatalog catalog;
  private TableIdentifier tableId;

  @Before
  public void setUp() {
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TABLE);
  }

  private static String json(Schema schema) {
    return SchemaParser.toJson(FileSchemas.canonical(schema));
  }

  private static CollectDistinctSchemas.SchemaGroup files(Schema schema, long count) {
    return files(schema, count, Collections.emptyList());
  }

  private static CollectDistinctSchemas.SchemaGroup files(
      Schema schema, long count, List<String> nullFreeColumns) {
    return CollectDistinctSchemas.SchemaGroup.of(json(schema), count, nullFreeColumns);
  }

  private long commit(
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      CollectDistinctSchemas.SchemaGroup... schemas) {
    return CommitSchemaUnion.commit(
        catalog,
        tableId,
        Arrays.asList(schemas),
        config,
        handling,
        CommitSchemaUnion.DEFAULT_COMMITTER);
  }

  private Table load() {
    return catalog.loadTable(tableId);
  }

  private static String metadataLocation(Table table) {
    return ((BaseTable) table).operations().current().metadataFileLocation();
  }

  /** The vN in .../metadata/vN.metadata.json: one commit bumps it by exactly one. */
  private static int metadataVersion(Table table) {
    String location = metadataLocation(table);
    String file = location.substring(location.lastIndexOf('/') + 2);
    return Integer.parseInt(file.substring(0, file.indexOf('.')));
  }

  /** A healthy mapping, so no-op tests are not turned into commits by the mapping repair. */
  private void seedNameMapping() {
    Table table = load();
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING, NameMappingUtils.regenerate(table.schema(), null))
        .commit();
  }

  // ---- no-op

  @Test
  public void testCoveredSchemasCommitNothing() {
    seedNameMapping();
    String before = metadataLocation(load());
    Schema covered =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "region", Types.StringType.get()));
    long schemaId = commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(covered, 3));
    Table table = load();
    assertEquals(table.schema().schemaId(), schemaId);
    assertEquals("no metadata written", before, metadataLocation(table));
  }

  // ---- changes applied

  @Test
  public void testAdditionPromotionAndRelaxationInOneCommit() {
    Schema file =
        new Schema(
            optional(1, "id", Types.LongType.get()),
            optional(2, "score", Types.DoubleType.get()),
            optional(3, "email", Types.StringType.get()),
            required(4, "region", Types.StringType.get()));
    long schemaId = commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 2));
    Table table = load();
    assertEquals(table.schema().schemaId(), schemaId);
    assertTrue(table.schema().findField("id").isOptional());
    assertEquals(Types.DoubleType.get(), table.schema().findField("score").type());
    assertNotNull(table.schema().findField("email"));
    assertTrue(table.schema().findField("email").isOptional());
  }

  @Test
  public void testAbsentRequiredColumnIsRelaxedExplicitly() {
    Schema file = new Schema(required(1, "id", Types.LongType.get()));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    assertTrue(load().schema().findField("region").isOptional());
  }

  @Test
  public void testFieldsInsideAnAddedStructAreOptional() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            required(
                3,
                "address",
                Types.StructType.of(
                    required(4, "city", Types.StringType.get()),
                    required(5, "zip", Types.IntegerType.get()))));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    Table table = load();
    assertTrue(table.schema().findField("address").isOptional());
    assertTrue(table.schema().findField("address.city").isOptional());
    assertTrue(table.schema().findField("address.zip").isOptional());
  }

  /**
   * Pins do not shape new columns: a pinned field arriving inside an added struct is relaxed like
   * any other. Pins keep existing required columns from being relaxed and gate files at
   * registration.
   */
  @Test
  public void testPinnedFieldInsideAnAddedStructIsStillAddedOptional() {
    SchemaEvolutionConfig pinned =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(Collections.singleton("address.city"))
            .build();
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            required(
                3, "address", Types.StructType.of(required(4, "city", Types.StringType.get()))));
    commit(pinned, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    assertTrue(load().schema().findField("address.city").isOptional());
    assertTrue(load().schema().findField("address").isOptional());
  }

  @Test
  public void testFieldsInsideAddedContainersAreOptional() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(
                3,
                "items",
                Types.ListType.ofRequired(
                    4, Types.StructType.of(required(5, "qty", Types.IntegerType.get())))),
            optional(
                6,
                "attrs",
                Types.MapType.ofRequired(
                    7,
                    8,
                    Types.StringType.get(),
                    Types.StructType.of(required(9, "v", Types.IntegerType.get())))));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    Schema schema = load().schema();
    assertTrue(schema.findField("items.element").isOptional());
    assertTrue(schema.findField("items.element.qty").isOptional());
    assertTrue(schema.findField("attrs.value").isOptional());
    assertTrue(schema.findField("attrs.value.v").isOptional());
  }

  // ---- newRequiredPaths (direct)

  @Test
  public void testNewRequiredPathsAtEveryLevelExceptMapKeys() {
    Schema before = new Schema(required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    required(3, "a", Types.IntegerType.get()),
                    optional(4, "b", Types.IntegerType.get()))),
            optional(
                5,
                "items",
                Types.ListType.ofRequired(
                    6, Types.StructType.of(required(7, "qty", Types.IntegerType.get())))),
            optional(
                8,
                "attrs",
                Types.MapType.ofRequired(
                    9,
                    10,
                    Types.StructType.of(required(11, "k", Types.StringType.get())),
                    Types.StructType.of(required(12, "v", Types.IntegerType.get())))));
    assertEquals(
        Arrays.asList("s.a", "items.element", "items.element.qty", "attrs.value", "attrs.value.v"),
        CommitSchemaUnion.newRequiredPaths(before, after));
  }

  /** Names containing element/key/value are not containers; regression for a substring check. */
  @Test
  public void testNewRequiredPathsContainerLikeNamesAreNotContainers() {
    Schema before = new Schema(required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "stats",
                Types.StructType.of(
                    required(3, "keyword", Types.StringType.get()),
                    required(4, "value_sum", Types.LongType.get()),
                    required(5, "element", Types.StringType.get()))));
    assertEquals(
        Arrays.asList("stats.keyword", "stats.value_sum", "stats.element"),
        CommitSchemaUnion.newRequiredPaths(before, after));
  }

  @Test
  public void testNewRequiredPathsInNestedContainers() {
    Schema before = new Schema(required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "ll",
                Types.ListType.ofRequired(
                    3, Types.ListType.ofRequired(4, Types.IntegerType.get()))),
            optional(
                5,
                "lm",
                Types.ListType.ofRequired(
                    6,
                    Types.MapType.ofRequired(
                        7, 8, Types.StringType.get(), Types.IntegerType.get()))));
    assertEquals(
        Arrays.asList("ll.element", "ll.element.element", "lm.element", "lm.element.value"),
        CommitSchemaUnion.newRequiredPaths(before, after));
  }

  /** Growing an existing struct: only the field with a new id is a candidate. */
  @Test
  public void testNewRequiredPathsIgnoreExistingFields() {
    Schema before =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "s", Types.StructType.of(required(3, "old", Types.IntegerType.get()))));
    Schema after =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    required(3, "old", Types.IntegerType.get()),
                    required(4, "fresh", Types.IntegerType.get()))));
    assertEquals(Arrays.asList("s.fresh"), CommitSchemaUnion.newRequiredPaths(before, after));
  }

  /** A declared-optional column every file proved null-free does not relax the table. */
  @Test
  public void testNullFreeColumnIsNotRelaxed() {
    seedNameMapping();
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "region", Types.StringType.get()));
    int before = metadataVersion(load());
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 3, Arrays.asList("region")));
    assertTrue(load().schema().findField("region").isRequired());
    assertEquals(before, metadataVersion(load()));
  }

  @Test
  public void testColumnWithoutNullFreeEvidenceStillRelaxes() {
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "region", Types.StringType.get()));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 3));
    assertTrue(load().schema().findField("region").isOptional());
  }

  @Test
  public void testMultipleSchemasMergeIntoOneSchemaVersion() {
    Schema a =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "a", Types.StringType.get()),
            optional(4, "shared", Types.StringType.get()));
    Schema b =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "b", Types.LongType.get()),
            optional(4, "shared", Types.StringType.get()));
    int before = metadataVersion(load());
    int schemasBefore = load().schemas().size();
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(a, 5), files(b, 1));
    Table table = load();
    assertNotNull(table.schema().findField("a"));
    assertNotNull(table.schema().findField("b"));
    assertNotNull(table.schema().findField("shared"));
    assertEquals("one metadata commit", before + 1, metadataVersion(table));
    assertEquals(
        "one schema version for the whole window", schemasBefore + 1, table.schemas().size());
  }

  /**
   * Columns born in the window negotiate their type among the window's files; the options guard
   * only columns the table already had. The winner is the widest type regardless of file counts.
   */
  @Test
  public void testOverlappingNewColumnsNegotiateTheWidestTypeInOneSchemaVersion() {
    Schema a =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.IntegerType.get()));
    Schema b =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.LongType.get()));
    int schemasBefore = load().schemas().size();
    commit(ADDITION_ONLY, IncompatibleSchemaHandling.FAIL_PIPELINE, files(a, 5), files(b, 1));
    Table table = load();
    assertEquals(Types.LongType.get(), table.schema().findField("code").type());
    assertEquals(schemasBefore + 1, table.schemas().size());
  }

  /** The narrower schema folding second is ignorable for the union, not a conflict. */
  @Test
  public void testOverlappingNewColumnsWidestFirstOrder() {
    Schema wide =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.LongType.get()));
    Schema narrow =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.IntegerType.get()));
    int schemasBefore = load().schemas().size();
    commit(
        ADDITION_ONLY, IncompatibleSchemaHandling.FAIL_PIPELINE, files(wide, 5), files(narrow, 1));
    Table table = load();
    assertEquals(Types.LongType.get(), table.schema().findField("code").type());
    assertEquals(schemasBefore + 1, table.schemas().size());
  }

  @Test
  public void testThreeOverlappingSchemasAllAccepted() {
    Schema narrow =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.IntegerType.get()));
    Schema wide =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.LongType.get()));
    Schema narrowExtra =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.IntegerType.get()),
            optional(4, "extra", Types.StringType.get()));
    commit(
        ADDITION_ONLY,
        IncompatibleSchemaHandling.FAIL_PIPELINE,
        files(narrow, 3),
        files(wide, 2),
        files(narrowExtra, 1));
    Table table = load();
    assertEquals(Types.LongType.get(), table.schema().findField("code").type());
    assertNotNull(table.schema().findField("extra"));
  }

  /**
   * Pins do not shape new columns: a pinned new top-level column is added optional like any other.
   * The pin is enforced per file at registration.
   */
  @Test
  public void testPinnedNewTopLevelColumnIsAddedOptional() {
    SchemaEvolutionConfig pinned =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(Collections.singleton("email"))
            .build();
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            required(3, "email", Types.StringType.get()));
    commit(pinned, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    assertTrue(load().schema().findField("email").isOptional());
  }

  /**
   * Because pinned new columns are added optional, a sibling schema lacking the pinned column is
   * not a schema-level problem: both schemas land, and the pin is enforced per file instead.
   */
  @Test
  public void testSiblingLackingPinnedNestedColumnIsStillCompatible() {
    SchemaEvolutionConfig pinned =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(Collections.singleton("address.city"))
            .build();
    Schema withCity =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(
                3, "address", Types.StructType.of(required(4, "city", Types.StringType.get()))));
    Schema withZipOnly =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(
                3, "address", Types.StructType.of(optional(4, "zip", Types.IntegerType.get()))));
    int schemasBefore = load().schemas().size();
    commit(
        pinned,
        IncompatibleSchemaHandling.FAIL_PIPELINE,
        files(withCity, 2),
        files(withZipOnly, 1));
    Table table = load();
    assertTrue(table.schema().findField("address.city").isOptional());
    assertTrue(table.schema().findField("address.zip").isOptional());
    assertEquals(schemasBefore + 1, table.schemas().size());
  }

  @Test
  public void testFinalSchemaIsOrderIndependent() {
    Schema a =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "score", Types.DoubleType.get()));
    Schema b =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "extra", Types.StringType.get()));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(a, 1), files(b, 2));
    Schema first = load().schema();

    TableIdentifier other = TableIdentifier.of("default", testName.getMethodName() + "_2");
    warehouse.createTable(other, TABLE);
    CommitSchemaUnion.commit(
        catalog,
        other,
        Arrays.asList(files(b, 2), files(a, 1)),
        ALL,
        IncompatibleSchemaHandling.FAIL_PIPELINE,
        CommitSchemaUnion.DEFAULT_COMMITTER);
    assertTrue(first.sameSchema(catalog.loadTable(other).schema()));
  }

  // ---- incompatible schemas

  @Test
  public void testFailPipelineCommitsNothingWhenAnySchemaIsIncompatible() {
    String before = metadataLocation(load());
    Schema good =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "email", Types.StringType.get()));
    Schema needsPromotion =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "score", Types.DoubleType.get()));
    IncompatibleSchemaException e =
        assertThrows(
            IncompatibleSchemaException.class,
            () ->
                commit(
                    ADDITION_ONLY,
                    IncompatibleSchemaHandling.FAIL_PIPELINE,
                    files(good, 9),
                    files(needsPromotion, 1)));
    assertTrue(e.getMessage(), e.getMessage().contains("1 schema(s), 1 file(s)"));
    assertTrue(e.getMessage(), e.getMessage().contains("promote score float to double"));
    assertEquals(before, metadataLocation(load()));
  }

  @Test
  public void testRouteToErrorsSkipsIncompatibleAndCommitsTheRest() {
    Schema good =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "email", Types.StringType.get()));
    Schema needsPromotion =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "score", Types.DoubleType.get()));
    commit(
        ADDITION_ONLY,
        IncompatibleSchemaHandling.ROUTE_TO_ERRORS,
        files(good, 9),
        files(needsPromotion, 1));
    Table table = load();
    assertNotNull(table.schema().findField("email"));
    assertEquals(Types.FloatType.get(), table.schema().findField("score").type());
  }

  @Test
  public void testPinnedRelaxationIsIncompatible() {
    SchemaEvolutionConfig pinned =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(Collections.singleton("region"))
            .build();
    Schema file = new Schema(required(1, "id", Types.LongType.get()));
    IncompatibleSchemaException e =
        assertThrows(
            IncompatibleSchemaException.class,
            () -> commit(pinned, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 4)));
    assertTrue(e.getMessage(), e.getMessage().contains("pinned as required"));
    assertTrue(load().schema().findField("region").isRequired());
  }

  @Test
  public void testMostCommonSchemaWinsAConflictBetweenFiles() {
    Schema asString =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.StringType.get()));
    Schema asLong =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.LongType.get()));
    commit(ALL, IncompatibleSchemaHandling.ROUTE_TO_ERRORS, files(asLong, 7), files(asString, 2));
    assertEquals(Types.LongType.get(), load().schema().findField("code").type());
  }

  @Test
  public void testConflictBetweenFilesFailsPipelineWithoutCommit() {
    String before = metadataLocation(load());
    Schema asString =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.StringType.get()));
    Schema asLong =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "code", Types.LongType.get()));
    IncompatibleSchemaException e =
        assertThrows(
            IncompatibleSchemaException.class,
            () ->
                commit(
                    ALL,
                    IncompatibleSchemaHandling.FAIL_PIPELINE,
                    files(asLong, 7),
                    files(asString, 2)));
    assertTrue(e.getMessage(), e.getMessage().contains("conflicts with another file schema"));
    assertEquals(before, metadataLocation(load()));
  }

  // ---- name mapping

  @Test
  public void testStaleNameMappingIsRegeneratedForTheNewSchema() {
    Table table = load();
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING, NameMappingUtils.regenerate(table.schema(), null))
        .commit();
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "email", Types.StringType.get()));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(file, 1));
    table = load();
    NameMapping mapping =
        NameMappingUtils.parseOrNull(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    assertNotNull(mapping);
    assertTrue(NameMappingUtils.covers(mapping, table.schema().asStruct()));
    assertNotNull(mapping.find("email"));
  }

  @Test
  public void testMissingNameMappingIsAddedEvenWithoutSchemaChanges() {
    assertFalse(load().properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING));
    commit(ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, files(TABLE, 1));
    Table table = load();
    NameMapping mapping =
        NameMappingUtils.parseOrNull(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    assertNotNull(mapping);
    assertTrue(NameMappingUtils.covers(mapping, table.schema().asStruct()));
  }

  // ---- retry

  @Test
  public void testCommitFailedOnceIsRetriedAgainstFreshState() {
    AtomicInteger attempts = new AtomicInteger();
    Committer flakyThenExternalChange =
        txn -> {
          if (attempts.incrementAndGet() == 1) {
            // someone else adds a column between our load and commit
            load().updateSchema().addColumn("external", Types.StringType.get()).commit();
            throw new CommitFailedException("simulated concurrent commit");
          }
          txn.commitTransaction();
        };
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "email", Types.StringType.get()));
    CommitSchemaUnion.commit(
        catalog,
        tableId,
        Arrays.asList(files(file, 1)),
        ALL,
        IncompatibleSchemaHandling.FAIL_PIPELINE,
        flakyThenExternalChange);
    Table table = load();
    assertEquals(2, attempts.get());
    assertNotNull(table.schema().findField("external"));
    assertNotNull(table.schema().findField("email"));
  }

  @Test
  public void testPersistentCommitFailurePropagates() {
    AtomicInteger attempts = new AtomicInteger();
    Committer alwaysFails =
        txn -> {
          attempts.incrementAndGet();
          throw new CommitFailedException("always");
        };
    Schema file =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "region", Types.StringType.get()),
            optional(3, "email", Types.StringType.get()));
    assertThrows(
        CommitFailedException.class,
        () ->
            CommitSchemaUnion.commit(
                catalog,
                tableId,
                Arrays.asList(files(file, 1)),
                ALL,
                IncompatibleSchemaHandling.FAIL_PIPELINE,
                alwaysFails));
    assertEquals(CommitSchemaUnion.MAX_ATTEMPTS, attempts.get());
  }

  @Test
  public void testEmptyInputCommitsNothing() {
    seedNameMapping();
    String before = metadataLocation(load());
    List<CollectDistinctSchemas.SchemaGroup> none = new ArrayList<>();
    CommitSchemaUnion.commit(
        catalog,
        tableId,
        none,
        ALL,
        IncompatibleSchemaHandling.FAIL_PIPELINE,
        CommitSchemaUnion.DEFAULT_COMMITTER);
    assertEquals(before, metadataLocation(load()));
  }
}
