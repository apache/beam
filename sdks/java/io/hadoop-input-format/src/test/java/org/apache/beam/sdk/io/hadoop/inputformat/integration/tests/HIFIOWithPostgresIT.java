package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs.DBInputWritable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs integration test to validate HadoopInputFromatIO for Postgres instance on GCP.
 *
 *
 * <p>
 * You can run just this test by doing the following: mvn test-Dtest=HIFIOWithPostgresIT.java
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithPostgresIT implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOWithPostgresIT.class);

  private static final String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver";
  private static String URL_PROPERTY = "jdbc:postgresql://";
  private static final String INPUT_TABLE_NAME_PROPERTY = "scientists";
  private static final String DATABASE_NAME = "beamdb";

  @Rule public final TestPipeline p = TestPipeline.create();
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
    LOGGER.info("Pipeline created successfully with the options");
    URL_PROPERTY += options.getServerIp() + ":" + String.format("%d", options.getServerPort()) + "/"
        + DATABASE_NAME;
  }

  @Test
  public void testReadData() throws IOException, InstantiationException, IllegalAccessException,
      ClassNotFoundException, InterruptedException {
    Configuration conf = getPostgresConfiguration();
    PCollection<KV<LongWritable, DBInputWritable>> postgresData =
        p.apply(HadoopInputFormatIO.<LongWritable, DBInputWritable>read().withConfiguration(conf));
    PAssert
        .thatSingleton(
            postgresData.apply("Count", Count.<KV<LongWritable, DBInputWritable>>globally()))
        .isEqualTo(10L);
    List<KV<LongWritable, DBInputWritable>> expectedResults =
        Arrays.asList(KV.of(new LongWritable(0L), new DBInputWritable("Faraday", "1")),
            KV.of(new LongWritable(1L), new DBInputWritable("Newton", "2")),
            KV.of(new LongWritable(2L), new DBInputWritable("Galilei", "3")),
            KV.of(new LongWritable(3L), new DBInputWritable("Maxwell", "4")),
            KV.of(new LongWritable(4L), new DBInputWritable("Pasteur", "5")),
            KV.of(new LongWritable(5L), new DBInputWritable("Copernicus", "6")),
            KV.of(new LongWritable(6L), new DBInputWritable("Curie", "7")),
            KV.of(new LongWritable(7L), new DBInputWritable("Bohr", "8")),
            KV.of(new LongWritable(8L), new DBInputWritable("Darwin", "9")),
            KV.of(new LongWritable(9L), new DBInputWritable("Einstein", "10")));
    PAssert.that(postgresData).containsInAnyOrder(expectedResults);
    p.run();
  }

  /**
   * Method to set Postgres specific configuration- driver class, jdbc url, username, password, and
   * table name
   */
  private static Configuration getPostgresConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS_PROPERTY);
    conf.set("mapreduce.jdbc.url", URL_PROPERTY);
    conf.set("mapreduce.jdbc.username", options.getUserName());
    conf.set("mapreduce.jdbc.password", options.getPassword());
    conf.set("mapreduce.jdbc.input.table.name", INPUT_TABLE_NAME_PROPERTY);
    conf.set("mapreduce.jdbc.input.query", "SELECT * FROM " + INPUT_TABLE_NAME_PROPERTY);
    conf.setClass("mapreduce.job.inputformat.class", DBInputFormat.class, InputFormat.class);
    conf.setClass("key.class", LongWritable.class, Object.class);
    conf.setClass("value.class", DBInputWritable.class, Object.class);
    Job job = Job.getInstance(conf);
    DBInputFormat.setInput(job,
        DBInputWritable.class,
        "scientists", // Input table name.
        null,
        null,
        new String[] {"name", "id"} // Table columns
    );
    return job.getConfiguration();
  }
}
