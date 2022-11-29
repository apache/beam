package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiPCollectionRowTupleTransform;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryStorageWriteApiSchemaTransformProviderTest {
  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  private static final Schema SCHEMA = Schema.of(
      Field.of("name", FieldType.STRING),
      Field.of("number", FieldType.INT64),
      Field.of("dt", FieldType.logicalType(SqlTypes.DATETIME)));

  private static final List<Row> ROWS = Arrays.asList(
      Row.withSchema(SCHEMA)
          .withFieldValue("name", "a")
          .withFieldValue("number", 1L)
          .withFieldValue("dt", LocalDateTime.parse("2000-01-01T00:00:00"))
          .build(),
      Row.withSchema(SCHEMA)
          .withFieldValue("name", "b")
          .withFieldValue("number", 2L)
          .withFieldValue("dt", LocalDateTime.parse("2000-01-02T00:00:00"))
          .build(),
      Row.withSchema(SCHEMA)
          .withFieldValue("name", "c")
          .withFieldValue("number", 3L)
          .withFieldValue("dt", LocalDateTime.parse("2000-01-03T00:00:00"))
          .build());

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Before
  public void setUp() throws Exception{
    FakeDatasetService.setUp();

    fakeDatasetService.createDataset("project", "dataset", "", "", null);
  }

  @Test
  public void testInvalidConfig() {
    List<BigQueryStorageWriteApiSchemaTransformConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setOutputTable("not_a_valid_table_spec"),
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setOutputTable("project:dataset.table")
                .setCreateDisposition("INVALID_DISPOSITION"),
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setOutputTable("project:dataset.table")
                .setJsonSchema("not a valid schema"),
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setOutputTable("create_table:without.schema")
                .setUseBeamSchema(false));

    for (BigQueryStorageWriteApiSchemaTransformConfiguration.Builder config: invalidConfigs) {
      assertThrows(Exception.class, () -> {config.build().validate();});
    }
  }

  public PCollectionRowTuple runWithConfig(BigQueryStorageWriteApiSchemaTransformConfiguration config) {
    BigQueryStorageWriteApiSchemaTransformProvider provider = new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiPCollectionRowTupleTransform writeRowTupleTransform =
        (BigQueryStorageWriteApiPCollectionRowTupleTransform) provider.from(config).buildTransform();

    writeRowTupleTransform.setBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);

    PCollection<Row> rows = p.apply(Create.of(ROWS).withRowSchema(SCHEMA));

    PCollectionRowTuple input = PCollectionRowTuple.of(tag, rows.setRowSchema(SCHEMA));
    PCollectionRowTuple result = input.apply(writeRowTupleTransform);

    return result;
  }

  @Test
  @Ignore
  public void testSimpleWrite() throws Exception{
    String tableSpec = "project:dataset.simple_write";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setOutputTable(tableSpec)
            .setUseBeamSchema(true)
            .build();

    runWithConfig(config);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertEquals(ROWS.size(), fakeDatasetService.getAllRows("project", "dataset", "simple_write").size());
  }

  @Test
  @Ignore
  public void testWithJsonSchema() throws Exception {
    String tableSpec = "project:dataset.with_json_schema";

    String jsonSchema =
          "{"
        + "  \"fields\": ["
        + "    {"
        + "      \"name\": \"name\","
        + "      \"type\": \"STRING\","
        + "      \"mode\": \"REQUIRED\""
        + "    },"
        + "    {"
        + "      \"name\": \"number\","
        + "      \"type\": \"INTEGER\","
        + "      \"mode\": \"REQUIRED\""
        + "    },"
        + "    {"
        + "      \"name\": \"dt\","
        + "      \"type\": \"DATETIME\","
        + "      \"mode\": \"REQUIRED\""
        + "    }"
        + "  ]"
        + "}";

    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setOutputTable(tableSpec)
            .setJsonSchema(jsonSchema)
            .setUseBeamSchema(false)
            .build();

    runWithConfig(config);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertEquals(ROWS.size(), fakeDatasetService.getAllRows("project", "dataset", "with_json_schema").size());
  }
}
