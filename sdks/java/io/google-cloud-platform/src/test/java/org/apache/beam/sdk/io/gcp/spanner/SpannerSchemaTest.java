package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.junit.Test;

/**
 * A test of {@link SpannerSchema}.
 */
public class SpannerSchemaTest {

  @Test
  public void testSingleTable() throws Exception {
    SpannerSchema schema = SpannerSchema.builder()
        .addColumn("test", "pk", "STRING(48)")
        .addKeyPart("test", "pk", false)
        .addColumn("test", "maxKey", "STRING(MAX)").build();

    assertEquals(1, schema.getTables().size());
    assertEquals(2, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());
  }

  @Test
  public void testTwoTables() throws Exception {
    SpannerSchema schema = SpannerSchema.builder()
        .addColumn("test", "pk", "STRING(48)")
        .addKeyPart("test", "pk", false)
        .addColumn("test", "maxKey", "STRING(MAX)")

        .addColumn("other", "pk", "INT64")
        .addKeyPart("other", "pk", true)
        .addColumn("other", "maxKey", "STRING(MAX)")

        .build();

    assertEquals(2, schema.getTables().size());
    assertEquals(2, schema.getColumns("test").size());
    assertEquals(1, schema.getKeyParts("test").size());

    assertEquals(2, schema.getColumns("other").size());
    assertEquals(1, schema.getKeyParts("other").size());
  }
}
