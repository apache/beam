package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration.JobType;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
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

  @Test
  public void testConfigurationSchema() {
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Schema got = provider.configurationSchema();
    assertEquals(SCHEMA, got);
  }

  @Test
  public void testFrom() {
    BigQuerySchemaIOConfiguration want = BigQuerySchemaIOConfiguration.builderOfQueryType().build();
    SchemaTransformProvider provider = new BigQuerySchemaIOProvider();
    Row inputConfig = Row.withSchema(provider.configurationSchema()).addValues(JobType.QUERY.name()).build();
    SchemaTransform schemaTransform = provider.from(inputConfig);
    BigQuerySchemaTransform bigQuerySchemaTransform = (BigQuerySchemaTransform) schemaTransform;
    BigQuerySchemaIOConfiguration got = bigQuerySchemaTransform.getConfiguration();
    assertEquals(want, got);
  }
}