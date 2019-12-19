package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import static org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataStoreReadWriteIT {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("created", DATETIME)
          .addNullableField("runDate", STRING)
          .addNullableField("status", STRING)
          .build();
  private static final String KIND = "batch";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testDataStoreV1SqlRead() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new DataStoreV1TableProvider());

    // TODO: replace with pipeline options
    String projectId = "google.com:clouddfe";

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   `created` TIMESTAMP, \n"
            + "   `runDate` VARCHAR, \n"
            + "   `status` VARCHAR \n"
            + ") \n"
            + "TYPE 'datastoreV1' \n"
            + "LOCATION '"
            + projectId + "/" + KIND
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(selectTableStatement));

    PipelineResult.State state = pipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }
}
