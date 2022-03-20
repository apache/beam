package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQuerySchemaIOProviderTest {

  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final Schema SCHEMA = AUTO_VALUE_SCHEMA.schemaFor(
      TypeDescriptor.of(BigQuerySchemaIOConfiguration.class));
  private static final SerializableFunction<BigQuerySchemaIOConfiguration, Row> ROW_SERIALIZABLE_FUNCTION =
      AUTO_VALUE_SCHEMA.toRowFunction(TypeDescriptor.of(BigQuerySchemaIOConfiguration.class));

  @Test
  public void testConfigurationSchema() {
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Schema got = provider.configurationSchema();
    assertEquals(SCHEMA, got);
  }

  @Test
  public void testFrom() {
    String query = "select * from example";
    BigQuerySchemaIOConfiguration want = BigQuerySchemaIOConfiguration.builderOfQueryType()
        .setQuery(query)
        .build();
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Row inputConfig = ROW_SERIALIZABLE_FUNCTION.apply(want);
    SchemaTransform schemaTransform = provider.from(inputConfig);
    BigQuerySchemaTransform bigQuerySchemaTransform = (BigQuerySchemaTransform) schemaTransform;
    BigQuerySchemaIOConfiguration got = bigQuerySchemaTransform.getConfiguration();
    assertEquals(want, got);
    assertEquals(query, got.getQuery());
  }
}