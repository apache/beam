package org.apache.beam.sdk.io.gcp.spanner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.Collections;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Sink. */
@RunWith(JUnit4.class)
public class SpannerWriteIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  public interface SpannerTestPipelineOptions extends TestPipelineOptions {
    @Description("Project ID for Spanner")
    @Default.String("apache-beam-testing")
    String getProjectId();

    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("beam-test")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Default.String("beam-testdb")
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Table name")
    @Default.String("users")
    String getTable();

    void setTable(String value);
  }

  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private SpannerTestPipelineOptions options;

  @Before
  public void setUp() throws Exception {
    PipelineOptionsFactory.register(SpannerTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SpannerTestPipelineOptions.class);

    spanner = SpannerOptions.newBuilder().setProjectId(options.getProjectId()).build().getService();

    databaseAdminClient = spanner.getDatabaseAdminClient();
    Operation<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(
            options.getInstanceId(),
            options.getDatabaseId(),
            Collections.singleton(
                "CREATE TABLE "
                    + options.getTable()
                    + " ("
                    + "  Key           INT64,"
                    + "  Value         STRING(MAX),"
                    + ") PRIMARY KEY (Key)"));
    op.waitFor();
  }

  @Test
  public void testWrite() throws Exception {
    p.apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new GenerateMutations(options.getTable())))
        .apply(
            SpannerIO.write()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(options.getDatabaseId()));

    p.run();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of(
                options.getProjectId(), options.getInstanceId(), options.getDatabaseId()));

    ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of("SELECT COUNT(*) FROM " + options.getTable()));
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getLong(0), equalTo(100L));
    assertThat(resultSet.next(), is(false));
  }

  @After
  public void tearDown() throws Exception {
    databaseAdminClient.dropDatabase(options.getInstanceId(), options.getDatabaseId());
    spanner.closeAsync().get();
  }

  private static class GenerateMutations extends DoFn<Long, Mutation> {
    private final String table;
    private final int valueSize = 100;

    public GenerateMutations(String table) {
      this.table = table;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
      Long key = c.element();
      builder.set("Key").to(key);
      builder.set("Value").to(RandomStringUtils.random(valueSize, true, true));
      Mutation mutation = builder.build();
      c.output(mutation);
    }
  }
}
