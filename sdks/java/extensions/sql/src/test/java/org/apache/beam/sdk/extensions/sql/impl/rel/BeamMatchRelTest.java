package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.apache.beam.sdk.schemas.Schema;

import static org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest.compilePipeline;
import static org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest.registerTable;

public class BeamMatchRelTest {

  public static final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void MatchLogicalPlanTest() {
    Schema schemaType = Schema.builder()
        .addInt32Field("id")
        .addStringField("name")
        .addInt32Field("proctime")
        .build();

    registerTable(
            "TestTable",
            TestBoundedTable.of(
                    schemaType)
                    .addRows(
                            1, "a", 1,
                            1, "b", 2,
                            1, "c", 3
                    ));

//
//    PCollection<Row> input =
//        pipeline.apply(
//            Create.of(
//                Row.withSchema(schemaType).addValues(
//                        1, "a", 1,
//                        1, "b", 2,
//                        1, "c", 3
//                ).build())
//                .withRowSchema(schemaType));

    String sql = "SELECT T.aid, T.bid, T.cid " +
        "FROM TestTable " +
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

//    PCollection<Row> result = input.apply(SqlTransform.query(sql));
    PCollection<Row> result = compilePipeline(sql, pipeline);

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Schema.FieldType.INT32, "id",
                Schema.FieldType.STRING, "name",
                Schema.FieldType.INT32, "proctime")
            .addRows(1, "a", 1, 1, "b", 2, 1, "c", 3)
            .getRows()
        );

    pipeline.run().waitUntilFinish();

  }
}
