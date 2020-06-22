package org.apache.beam.sdk.extensions.sql.utils;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.apache.beam.sdk.schemas.Schema;

public class BeamSqlDslMatchTest {

  public static final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void MatchLogicalPlanTest() {
    Schema schemaType = Schema.builder()
        .addInt32Field("id")
        .addStringField("name")
        .addInt32Field("proctime")
        .build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                Row.withSchema(schemaType).addValue(1).addValue("a").addValue(1).build())
                .withRowSchema(schemaType));

    String sql = "SELECT T.aid, T.bid, T.cid " +
        "FROM PCOLLECTION " +
        "MATCH_RECOGNIZE (" +
        "PARTITION BY id " +
        "ORDER BY proctime " +
        "MEASURES " +
        "A.id AS aid, " +
        "B.id AS bid, " +
        "C.id AS cid " +
        "PATTERN (A B C) " +
        "DEFINE " +
        "A AS name = 'a', " +
        "B AS name = 'b', " +
        "C AS name = 'c' " +
        ") AS T";

    PCollection<Row> result = input.apply(SqlTransform.query(sql));

    pipeline.run();

  }
}
