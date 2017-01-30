package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
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

	  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOWithPostgresIT.class);

	private static final String DRIVER_CLASS_PROPERTY = "org.postgresql.Driver";
	private static String URL_PROPERTY = "jdbc:postgresql://";
//	private static final String USERNAME_PROPERTY = "postgres";
//	private static final String PASSWORD_PROPERTY = "pslpvtltd";
	private static final String INPUT_TABLE_NAME_PROPERTY = "scientists";
	private static final String DATABASE_NAME="beamdb";
	@Rule
	public final TestPipeline p = TestPipeline.create();
	private static HIFTestOptions options;
	

	@BeforeClass
	public static void setUp() {
		PipelineOptionsFactory.register(HIFTestOptions.class);
		options = TestPipeline.testingPipelineOptions()
				.as(HIFTestOptions.class);
		LOGGER.info("Pipeline created successfully with the options");
		URL_PROPERTY+=options.getServerIp()+":"+String.format("%d",options.getServerPort())+"/"+DATABASE_NAME;
	}

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
				.isEqualTo(50L);
		//TODO add test for checking the contents
		p.run();
	}
	
	/**
	 * Method to set Postgres specific configuration- driver class, jdbc url, 
	 * username, password, and table name
	 */
	private Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("mapreduce.jdbc.driver.class", DRIVER_CLASS_PROPERTY);
		conf.set("mapreduce.jdbc.url", URL_PROPERTY);
		conf.set("mapreduce.jdbc.username", options.getUserName());
		conf.set("mapreduce.jdbc.password", options.getPassword());
		conf.set("mapreduce.jdbc.input.table.name", INPUT_TABLE_NAME_PROPERTY);
		conf.set("mapreduce.jdbc.input.query", "SELECT * FROM "
				+ INPUT_TABLE_NAME_PROPERTY);
		conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
				org.apache.hadoop.mapreduce.lib.db.DBInputFormat.class,
				InputFormat.class);
		conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, org.apache.hadoop.io.LongWritable.class,
				Object.class);
		conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, DBInputWritable.class, Object.class);
		return conf;
	}

}
