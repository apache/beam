package org.apache.beam.sdk.extensions.sql.zetasql;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for STRING_AGG variants:
 * - STRING_AGG(STRING)
 * - STRING_AGG(STRING, delim)
 * - STRING_AGG(BYTES)
 * - STRING_AGG(BYTES, delim)
 */
@RunWith(JUnit4.class)
public class ZetaSqlStringAggTest extends ZetaSqlTestBase {
  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testStringAggregationNoDelim() {
    String sql =
        "SELECT STRING_AGG(fruit) AS aggregated"
            + " FROM UNNEST(['apple', 'pear', 'banana', 'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple,pear,banana,pear").build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testStringAggregationWithStringDelim() {
    String sql =
        "SELECT STRING_AGG(fruit, '|') AS aggregated"
            + " FROM UNNEST(['apple', 'pear', 'banana', 'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple|pear|banana|pear").build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testStringAggregationWithBytesDelim() {
    String sql =
        "SELECT STRING_AGG(fruit, b'|') AS aggregated"
            + " FROM UNNEST(['apple', 'pear', 'banana', 'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple|pear|banana|pear").build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testBytesAggregationNoDelim() {
    String sql =
        "SELECT STRING_AGG(fruit) AS aggregated"
            + " FROM UNNEST([b'apple', b'pear', b'banana', b'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple,pear,banana,pear".getBytes(UTF_8)).build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testBytesAggregationWithStringDelim() {
    String sql =
        "SELECT STRING_AGG(fruit, '|') AS aggregated"
            + " FROM UNNEST([b'apple', b'pear', b'banana', b'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple|pear|banana|pear".getBytes(UTF_8)).build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testBytesAggregationWithBytesDelim() {
    String sql =
        "SELECT STRING_AGG(fruit, b'|') AS aggregated"
            + " FROM UNNEST([b'apple', b'pear', b'banana', b'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple|pear|banana|pear".getBytes(UTF_8)).build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }

  @Test
  public void testBytesAggregationWithNonUtf8BytesDelim() {
    String sql =
        "SELECT STRING_AGG(fruit, b'|') AS aggregated"
            + " FROM UNNEST([b'apple', b'pear', b'banana', b'pear']) AS fruit";
    PCollection<Row> stream = execute(sql);

    Schema schema = Schema.builder().addStringField("aggregated").build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValue("apple|pear|banana|pear".getBytes(UTF_8)).build());

    pipeline.run().waitUntilFinish(PIPELINE_EXECUTION_WAITTIME);
  }
}
