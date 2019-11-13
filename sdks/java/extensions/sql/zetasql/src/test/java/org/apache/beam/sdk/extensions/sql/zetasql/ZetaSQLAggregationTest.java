package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;

public class ZetaSQLAggregationTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.fromOptions(createPipelineOptions());

  private static PipelineOptions createPipelineOptions() {
    BeamSqlPipelineOptions opts = PipelineOptionsFactory.create().as(BeamSqlPipelineOptions.class);
    opts.setPlannerName(ZetaSQLQueryPlanner.class.getName());
    return opts;
  }

  @Test
  public void testAggregate() {
    Schema inputSchema = Schema.builder()
        .addByteArrayField("id")
        .addInt64Field("has_f1")
        .addInt64Field("has_f2")
        .addInt64Field("has_f3")
        .addInt64Field("has_f4")
        .addInt64Field("has_f5")
        .addInt64Field("has_f6")
        .build();

    String sql = "SELECT \n" +
        "  id, \n" +
        "  COUNT(*) as count, \n" +
        "  SUM(has_f1) as f1_count, \n" +
        "  SUM(has_f2) as f2_count, \n" +
        "  SUM(has_f3) as f3_count, \n" +
        "  SUM(has_f4) as f4_count, \n" +
        "  SUM(has_f5) as f5_count, \n" +
        "  SUM(has_f6) as f6_count  \n" +
        "FROM PCOLLECTION \n" +
        "GROUP BY id";

    pipeline
        .apply(Create.empty(inputSchema))
        .apply(SqlTransform.query(sql));

    pipeline.run();
  }

  @Test
  public void testAggregate2() {
    Schema inputSchema = Schema.builder()
        .addByteArrayField("id")
        .addInt64Field("has_f1")
        .addInt64Field("has_f2")
        .addInt64Field("has_f3")
        .addInt64Field("has_f4")
        .addInt64Field("has_f5")
        .addInt64Field("has_f6")
        .build();

    String sql = "SELECT \n" +
        "  id, \n" +
        "  SUM(has_f1) as f1_count, \n" +
        "  SUM(has_f2) as f2_count, \n" +
        "  SUM(has_f3) as f3_count, \n" +
        "  SUM(has_f4) as f4_count, \n" +
        "  SUM(has_f5) as f5_count, \n" +
        "  SUM(has_f6) as f6_count  \n" +
        "FROM PCOLLECTION \n" +
        "GROUP BY id";

    pipeline
        .apply(Create.empty(inputSchema))
        .apply(SqlTransform.query(sql));

    pipeline.run();
  }

}
