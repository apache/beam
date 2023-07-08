package org.apache.beam.sdk.io.jdbc;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.ds.PGSimpleDataSource;
import org.apache.beam.sdk.values.KV;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.junit.Assert.assertNotNull;

/**
 * This class contains integration tests for {@link JdbcIO} with {@link JdbcIO.DataSourceConfiguration}
 * and {@link JdbcIO.StatementPreparator} using {@link org.apache.beam.sdk.io.jdbc.JdbcIOToAvroIT#COLUMNS}.
 * <p>
 * The tests are parameterized by the column under test.
 *
 * to run this test, you need to run the following command:
 * ./gradlew integrationTest -p sdks/java/io/jdbc -DintegrationTestPipelineOptions='["--postgresServerName=localhost","--postgresUsername=guest","--postgresDatabaseName=guest","--postgresPassword=postgres","--postgresSsl=false" ]' --tests org.apache.beam.sdk.io.jdbc.JdbcIOToAvroIT -DintegrationTestRunner=direct
 *
 */
@RunWith(Parameterized.class)
public class JdbcIOToAvroIT {

    public static final ArrayList<KV<String, String>> COLUMNS = Lists.newArrayList(
            KV.of("num_nul", "NUMERIC NULL"),
            KV.of("num_notnul", "NUMERIC NOT NULL"),
            KV.of("num_ps_nul", "NUMERIC(10,2) NULL"),
            KV.of("num_ps_notnul", "NUMERIC(10,2) NOT NULL"),
            KV.of("dec_nul", "DECIMAL NULL"),
            KV.of("dec_notnul", "DECIMAL NOT NULL"),
            KV.of("dec_ps_nul", "DECIMAL(10,2) NULL"),
            KV.of("dec_ps_notnul", "DECIMAL(10,2) NOT NULL"),
            KV.of("int", "INT NOT NULL")
    );
    private static PGSimpleDataSource dataSource;
    private static String tableName;
    private final String columnUnderTest;
    @Rule
    public TestPipeline pipelineDecimals = TestPipeline.create();


    @BeforeClass
    public static void setup() throws SQLException {
        PostgresIOTestPipelineOptions options;
        try {
            options = readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);
        } catch (IllegalArgumentException e) {
            options = null;
        }
        org.junit.Assume.assumeNotNull(options);

        dataSource = DatabaseTestHelper.getPostgresDataSource(options);
        tableName = DatabaseTestHelper.getTestTableName(JdbcIOToAvroIT.class.getSimpleName());


        DatabaseTestHelper.createTable(
                dataSource,
                tableName,
                COLUMNS);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("insert into %s values (%s,%s,%s,%s,%s,%s,%s,%s    ,%s)," +
                                "(%s,%s,%s,%s,%s,%s,%s,%s    ,%s) ", tableName,
                        1,2,"3.45","4.56",      1,2,"3.45","4.56",    "1",
                        "NULL",2,"NULL","4.56", "NULL",2,"NULL","4.56",   "2"));
            }
        }

    }

    @AfterClass
    public static void tearDown() throws SQLException {
        DatabaseTestHelper.deleteTable(dataSource, tableName);
    }

    @Parameterized.Parameters(name = "{index}: {0} {1}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        for (KV<String, String> column : COLUMNS) {
            data.add(new Object[]{column.getKey(), column.getValue()});
        }
        return data;
    }

    public JdbcIOToAvroIT(String columnName, String ignore) {
        this.columnUnderTest = columnName;
    }

    @Test
    public void testColumn() {
        PCollection<Row> jdbcRows = pipelineDecimals
                .apply(JdbcIO.readRows()
                        .withQuery("SELECT " + columnUnderTest +                                " FROM " + tableName)
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource)));
        Schema schema = jdbcRows.getSchema();
        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
        assertNotNull( avroSchema.getField(columnUnderTest));
        PAssert.thatSingleton(jdbcRows.apply("Count All", Count.globally()))
                .isEqualTo((long) 2);
        PipelineResult pipelineResult = pipelineDecimals.run();
        pipelineResult.waitUntilFinish();
    }
}
