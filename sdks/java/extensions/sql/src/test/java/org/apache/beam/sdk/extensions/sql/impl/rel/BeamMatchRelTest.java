package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.apache.beam.sdk.schemas.Schema;

public class BeamMatchRelTest {

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
        "A AS PREV(name) = 'mark', " +
        "B AS NEXT(name) = 'andy', " +
        "C AS name = 'tagore' " +
        ") AS T";

    PCollection<Row> result = input.apply(SqlTransform.query(sql));

    pipeline.run();

  }
}
