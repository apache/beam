package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

/**
 * Runs integration test to validate HadoopInputFromatIO for Postgres instance
 * on GCP.
 *
 *
 * <p>
 * You can run just this test by doing the following: mvn
 * test-Dtest=HIFIOWithPostgresIT.java
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithPostgresIT implements Serializable {

	/*
	 * private static final long serialVersionUID = 1L; private static final
	 * String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver"; private static
	 * final String URL_PROPERTY =
	 * "jdbc:postgresql://104.197.246.66:5432/postgres"; private static final
	 * String USERNAME_PROPERTY = "postgres"; private static final String
	 * PASSWORD_PROPERTY = "euthS3M1"; private static final String
	 * INPUT_TABLE_NAME_PROPERTY = "mytable";
	 */
	private static final String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver";
	private static final String URL_PROPERTY = "jdbc:postgresql://localhost:5432/beamdb";
	private static final String USERNAME_PROPERTY = "postgres";
	private static final String PASSWORD_PROPERTY = "pslpvtltd";
	private static final String INPUT_TABLE_NAME_PROPERTY = "beamtable";
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
				.isEqualTo(3L);

		p.run();
	}

	private Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS_PROPERTY);
		conf.set("mapreduce.jdbc.url", URL_PROPERTY);
		conf.set("mapreduce.jdbc.username", USERNAME_PROPERTY);
		conf.set("mapreduce.jdbc.password", PASSWORD_PROPERTY);
		conf.set("mapreduce.jdbc.input.table.name", INPUT_TABLE_NAME_PROPERTY);
		conf.set("mapreduce.jdbc.input.query", "SELECT * FROM "
				+ INPUT_TABLE_NAME_PROPERTY);
		// conf.set("mapreduce.jdbc.input.count.query",
		// "SELECT count(*) FROM mytable");
		conf.setClass("mapreduce.job.inputformat.class",
				org.apache.hadoop.mapreduce.lib.db.DBInputFormat.class,
				InputFormat.class);
		conf.setClass("key.class", org.apache.hadoop.io.LongWritable.class,
				Object.class);
		conf.setClass("value.class", DBInputWritable.class, Object.class);
		return conf;
	}

}

