package org.apache.beam.sdk.io.influxdb;

import org.apache.beam.sdk.PipelineResult;

import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;


/**
 * A test of {@link org.apache.beam.sdk.io.influxdb.InfluxDBIO} on an independent InfluxDB instance.
 *
 * <p>This test requires a running instance of InfluxDB. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/influxdb -DintegrationTestPipelineOptions='[
 *  "--influxdburl=http://localhost:8086",
 *  "--infuxDBDatabase=mypass",
 *  "--username=username"
 *  "--password=password"]'
 *  --tests org.apache.beam.sdk.io.influxdb.InfluxDBIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 */


@RunWith(JUnit4.class)
public class InfluxDBIOIT {

    private static InfluxDBPipelineOptions options;

    @Rule
    public final TestPipeline writePipeline = TestPipeline.create();
    @Rule
    public final TestPipeline readPipeline = TestPipeline.create();

    /**
     * InfluxDBIO options.
     */
    public interface InfluxDBPipelineOptions extends IOTestPipelineOptions {
        @Description("InfluxDB host (host name/ip address)")
        @Default.String("http://localhost:8086")
        String getInfluxDBURL();
        void setInfluxDBURL(String value);

        @Description("Username for InfluxDB")
        @Default.String("superadmin")
        String getInfluxDBUserName();
        void setInfluxDBUserName(String value);

        @Description("Password for InfluxDB")
        @Default.String("supersecretpassword")
        String getInfluxDBPassword();
        void setInfluxDBPassword(String value);

        @Description("InfluxDB database name")
        @Default.String("db0")
        String getDatabaseName();
        void setDatabaseName(String value);

    }
    @BeforeClass
    public static void setUp() {
        PipelineOptionsFactory.register(InfluxDBPipelineOptions.class);
        options = TestPipeline.testingPipelineOptions().as(InfluxDBPipelineOptions.class);
    }

    @After
    public  void clear(){
        try (InfluxDB connection = InfluxDBFactory.connect(options.getInfluxDBURL(),
                options.getInfluxDBUserName(), options.getInfluxDBPassword())){
            connection.query(new Query("DROP DATABASE \"" + options.getDatabaseName() + "\""));
        }
        }
    @Before
    public  void initTest(){
        try (InfluxDB connection = InfluxDBFactory.connect(options.getInfluxDBURL(),
                options.getInfluxDBUserName(), options.getInfluxDBPassword())){
            connection.query(new Query("CREATE DATABASE \"" + options.getDatabaseName() + "\""));
        }
    }
    @Test
    public void testWriteAndRead() {
        final int noofElementsToReadAndWrite = 1000;
        writePipeline
                .apply("Generate data", Create.of(GenerateData.getMetric(noofElementsToReadAndWrite)))
                .apply(
                        "Write data to InfluxDB",
                        InfluxDBIO.write()
                                .withConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                        options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                .withDatabase(options.getDatabaseName())
                                .withSslInvalidHostNameAllowed(false)
                               .withSslEnabled(false));
        writePipeline.run().waitUntilFinish();
        PCollection<String> readVals =
                readPipeline
                        .apply(
                                "Read all points in Influxdb",
                                InfluxDBIO.read()
                                        .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                                options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                        .withDatabase(options.getDatabaseName()).withQuery("SELECT * FROM \"test_m\"")
                                        .withSslInvalidHostNameAllowed(false)
                                        .withSslEnabled(false));

        PAssert.thatSingleton(readVals.apply("Count All", Count.globally()))
                .isEqualTo((long) noofElementsToReadAndWrite);
        PipelineResult readResult = readPipeline.run();
        readResult.waitUntilFinish();
    }

    @Test
    public void testWriteAndReadWithSingleMetric() {
        final int noofElementsToReadAndWrite = 1000;
        writePipeline
                .apply("Generate data", Create.of(GenerateData.getMetric(noofElementsToReadAndWrite)))
                .apply(
                        "Write data to InfluxDB",
                        InfluxDBIO.write()
                                .withConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                        options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                .withDatabase(options.getDatabaseName())
                                .withSslInvalidHostNameAllowed(false)
                                .withSslEnabled(false));
        writePipeline.run().waitUntilFinish();
        PCollection<String> readVals =
                readPipeline
                        .apply(
                                "Read all points in InfluxDB",
                                InfluxDBIO.read()
                                        .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                                options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                        .withDatabase(options.getDatabaseName()).withMetric(Arrays.asList("test_m"))
                                        .withSslInvalidHostNameAllowed(false)
                                        .withSslEnabled(false));

        PAssert.thatSingleton(readVals.apply("Count All", Count.globally()))
                .isEqualTo((long) noofElementsToReadAndWrite);
        PipelineResult readResult = readPipeline.run();
        readResult.waitUntilFinish();
    }

    @Test
    public void testWriteAndReadWithMultipleMetric() {
        final int noofElementsToReadAndWrite = 1000;
        writePipeline
                .apply("Generate data", Create.of(GenerateData.getMultipleMetric(noofElementsToReadAndWrite)))
                .apply(
                        "Write data to InfluxDB",
                        InfluxDBIO.write()
                                .withConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                        options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                .withDatabase(options.getDatabaseName())
                                .withSslInvalidHostNameAllowed(false)
                                .withSslEnabled(false));
        writePipeline.run().waitUntilFinish();
        PCollection<String> readVals =
                readPipeline
                        .apply(
                                "Read all points in InfluxDB",
                                InfluxDBIO.read()
                                        .withDataSourceConfiguration(InfluxDBIO.DataSourceConfiguration.create(options.getInfluxDBURL(),
                                                options.getInfluxDBUserName(), options.getInfluxDBPassword()))
                                        .withDatabase(options.getDatabaseName()).withMetric(Arrays.asList("test_m", "test_m1"))
                                        .withSslInvalidHostNameAllowed(false)
                                        .withSslEnabled(false));

        PAssert.thatSingleton(readVals.apply("Count All", Count.globally()))
                .isEqualTo((long) noofElementsToReadAndWrite*2);
        PipelineResult readResult = readPipeline.run();
        readResult.waitUntilFinish();
    }

}
