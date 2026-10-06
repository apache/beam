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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Planning without committing; what a plan does once committed is CommitSchemaUnionTest's. */
@RunWith(JUnit4.class)
public class SchemaPlanTest {
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

  private static final CommitSchemaUnion.Settings SETTINGS =
      new CommitSchemaUnion.Settings(
          ALL,
          IncompatibleSchemaHandling.FAIL_PIPELINE,
          new CommitSchemaUnion.NewTableSettings(null, null, null));

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
    return CollectDistinctSchemas.SchemaGroup.of(json(schema), count, Collections.emptyList());
  }

  private Table load() {
    return catalog.loadTable(tableId);
  }

  /** Full-schema comparison, field ids normalized; string form so a failure shows the diff. */
  private static void assertSameSchema(Schema expected, Schema actual) {
    assertEquals(
        TypeUtil.assignIncreasingFreshIds(expected).asStruct().toString(),
        TypeUtil.assignIncreasingFreshIds(actual).asStruct().toString());
  }

  // ---- plans

  /** The mapping repair is a plan decision, made before anything is committed. */
  @Test
  public void testPlanReportsTheNameMappingRepair() {
    Table table = load();
    assertFalse(table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING));
    List<CollectDistinctSchemas.SchemaGroup> covered = Arrays.asList(files(TABLE, 1));
    SchemaPlan.Evolution plan =
        (SchemaPlan.Evolution) SchemaPlan.compute(catalog, tableId, table, covered, SETTINGS);
    assertNull(plan.newSchema);
    assertTrue(plan.repairsNameMapping);

    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING, NameMappingUtils.regenerate(table.schema(), null))
        .commit();
    plan = (SchemaPlan.Evolution) SchemaPlan.compute(catalog, tableId, load(), covered, SETTINGS);
    assertFalse(plan.repairsNameMapping);
  }

  @Test
  public void testPlanForCreationListsAcceptedSchemasAndCreationSettings() {
    TableIdentifier missing = TableIdentifier.of("default", testName.getMethodName() + "_new");
    Schema seed =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "region", Types.StringType.get()));
    Schema other =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "extra", Types.LongType.get()));
    CommitSchemaUnion.NewTableSettings creation =
        new CommitSchemaUnion.NewTableSettings(Arrays.asList("region"), Arrays.asList("id"), null);
    SchemaPlan.Creation plan =
        (SchemaPlan.Creation)
            SchemaPlan.compute(
                catalog,
                missing,
                null,
                Arrays.asList(files(seed, 5), files(other, 1)),
                new CommitSchemaUnion.Settings(
                    ALL, IncompatibleSchemaHandling.FAIL_PIPELINE, creation));
    assertTrue(plan.canCreate());
    assertEquals(2, plan.schemasToMerge.size());
    assertEquals(5, checkStateNotNull(plan.toMerge(json(seed))).files);
    assertEquals(1, checkStateNotNull(plan.toMerge(json(other))).files);
    assertEquals("region", checkStateNotNull(plan.spec).fields().get(0).name());
    assertEquals(1, checkStateNotNull(plan.sortOrder).fields().size());
    assertFalse(catalog.tableExists(missing));
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
        SchemaPlan.newRequiredPaths(before, after));
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
        SchemaPlan.newRequiredPaths(before, after));
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
        SchemaPlan.newRequiredPaths(before, after));
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
    assertEquals(Arrays.asList("s.fresh"), SchemaPlan.newRequiredPaths(before, after));
  }

  // ---- createdSchema (direct)

  @Test
  public void testCreatedSchemaPinsHoldAtEveryLevel() {
    Schema merged =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "l",
                Types.ListType.ofOptional(
                    3, Types.StructType.of(optional(4, "q", Types.IntegerType.get())))));
    SchemaEvolutionConfig pinned =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(Collections.singleton("l.element.q"))
            .build();
    Schema created = SchemaPlan.createdSchema(merged, pinned);
    assertSameSchema(
        new Schema(
            optional(1, "id", Types.LongType.get()),
            required(
                2,
                "l",
                Types.ListType.ofRequired(
                    3, Types.StructType.of(required(4, "q", Types.IntegerType.get()))))),
        created);
  }

  @Test
  public void testCreatedSchemaEveryLevelOptionalExceptMapKeys() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "s",
                Types.StructType.of(
                    required(3, "a", Types.IntegerType.get()),
                    required(
                        4,
                        "items",
                        Types.ListType.ofRequired(
                            5, Types.StructType.of(required(6, "qty", Types.IntegerType.get())))))),
            required(
                7,
                "attrs",
                Types.MapType.ofRequired(
                    8,
                    9,
                    Types.StructType.of(required(10, "k", Types.StringType.get())),
                    Types.StructType.of(required(11, "v", Types.IntegerType.get())))));
    assertSameSchema(
        new Schema(
            optional(1, "id", Types.LongType.get()),
            optional(
                2,
                "s",
                Types.StructType.of(
                    optional(3, "a", Types.IntegerType.get()),
                    optional(
                        4,
                        "items",
                        Types.ListType.ofOptional(
                            5, Types.StructType.of(optional(6, "qty", Types.IntegerType.get())))))),
            optional(
                7,
                "attrs",
                Types.MapType.ofOptional(
                    8,
                    9,
                    Types.StructType.of(required(10, "k", Types.StringType.get())),
                    Types.StructType.of(optional(11, "v", Types.IntegerType.get()))))),
        SchemaPlan.createdSchema(schema, ALL));
  }
}
