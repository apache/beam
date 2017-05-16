package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableTestOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class SpannerWriteIT {

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
        options = TestPipeline.testingPipelineOptions().as
                (SpannerTestPipelineOptions.class);

        spanner = SpannerOptions.newBuilder().setProjectId(options.getProjectId())
                .build().getService();

        databaseAdminClient = spanner.getDatabaseAdminClient();
        Operation<Database, CreateDatabaseMetadata> op = databaseAdminClient.createDatabase(
                options.getInstanceId(), options
                        .getDatabaseId(), Collections.singleton("CREATE TABLE " + options.getTable() + " ("
                        + "  Key           INT64,"
                        + "  Name          STRING,"
                        + "  Email         STRING,"
                        + "  Age           INT,"
                        + ") PRIMARY KEY (Key)"));
        op.waitFor();
    }

    @Test
    public void testWrite() throws Exception {
        
    }

    @After
    public void tearDown() throws Exception {
        databaseAdminClient.dropDatabase(options.getInstanceId(), options
                .getDatabaseId());

    }
}
