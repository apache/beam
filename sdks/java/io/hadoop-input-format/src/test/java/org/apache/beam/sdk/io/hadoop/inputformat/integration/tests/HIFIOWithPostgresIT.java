package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

/**
 * Runs integration test to validate HadoopInputFromatIO for Postgres instance on GCP.
 *
 *
 * <p>
 * You can run just this test by doing the following:
 * mvn test-Dtest=HIFIOWithPostgresIT.java
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithPostgresIT implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver";
	private static final String URL_PROPERTY = "jdbc:postgresql://104.197.246.66:5432/postgres";
	private static final String USERNAME_PROPERTY = "postgres";
	private static final String PASSWORD_PROPERTY = "euthS3M1";
	private static final String INPUT_TABLE_NAME_PROPERTY = "mytable";

	@Rule
	public final TestPipeline p = TestPipeline.create();

	@Test
	public void testReadData() throws IOException, InstantiationException,
			IllegalAccessException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = getConfiguration();

		PCollection<KV<Text, IntWritable>> postgresData = p
				.apply(HadoopInputFormatIO.<Text, IntWritable> read()
						.withConfiguration(conf));
		PAssert.thatSingleton(
				postgresData.apply("Count",
						Count.<KV<Text, IntWritable>> globally()))
				.isEqualTo(4L);

		p.run();
	}

	private Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS_PROPERTY);
		conf.set("mapreduce.jdbc.url", URL_PROPERTY);
		conf.set("mapreduce.jdbc.username", USERNAME_PROPERTY);
		conf.set("mapreduce.jdbc.password", PASSWORD_PROPERTY);
		conf.set("mapreduce.jdbc.input.table.name", INPUT_TABLE_NAME_PROPERTY);
		conf.set("mapreduce.jdbc.input.query", "SELECT * FROM mytable");
		conf.set("mapreduce.jdbc.input.count.query",
				"SELECT count(*) FROM mytable");
		conf.setClass("mapreduce.job.inputformat.class",
				org.apache.hadoop.mapreduce.lib.db.DBInputFormat.class,
				InputFormat.class);
		conf.setClass("key.class", org.apache.hadoop.io.Text.class,
				Object.class);
		conf.setClass("value.class", org.apache.hadoop.io.IntWritable.class,
				Object.class);
		return conf;
	}

}
