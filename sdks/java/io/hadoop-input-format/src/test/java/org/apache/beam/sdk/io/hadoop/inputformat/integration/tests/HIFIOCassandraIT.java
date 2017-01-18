/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOContants;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

/**
 * Runs integration test to validate HadoopInputFromatIO for a Cassandra
 * instance on GCP.
 *
 * You need to pass Cassandra server IP and port in beamTestPipelineOptions
 *
 * <p>
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]'
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOCassandraIT implements Serializable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HIFIOCassandraIT.class);
	private static final String CASSANDRA_KEYSPACE = "ycsb";
	private static final String CASSANDRA_TABLE = "usertable";
	private static Configuration conf;
	private static HIFTestOptions options;

	@BeforeClass
	public static void setUp() {
		PipelineOptionsFactory.register(HIFTestOptions.class);
		options = TestPipeline.testingPipelineOptions()
				.as(HIFTestOptions.class);
		LOGGER.info("Pipeline created successfully with the options.");
		conf = getConfiguration(options);
	}

	/**
	 * This test reads data from the Cassandra instance and verifies if data is
	 * read successfully.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandra() throws Exception {
		Pipeline pipeline = TestPipeline.create(options);
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String apply(Row input) {
				return input.getString("y_id");
			}
		};
		PCollection<KV<Long, String>> cassandraData = pipeline
				.apply(HadoopInputFormatIO.<Long, String> read()
						.withConfiguration(conf)
						.withValueTranslation(myValueTranslate));
		PAssert.thatSingleton(
				cassandraData.apply("Count",
						Count.<KV<Long, String>> globally())).isEqualTo(1000L);
		pipeline.run();
	}

	/**
	 * This test reads data from the Cassandra instance based on query and
	 * verifies if data is read successfully.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandraQuery() throws Exception {
		Pipeline pipeline = TestPipeline.create(options);
		conf.set(
				"cassandra.input.cql",
				"select * from ycsb.usertable where token(y_id) > ? and token(y_id) <= ? and y_id='user3117720508089767496' allow filtering");
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String apply(Row input) {
				return input.getString("y_id");
			}
		};
		PCollection<KV<Long, String>> cassandraData = pipeline
				.apply(HadoopInputFormatIO.<Long, String> read()
						.withConfiguration(conf)
						.withValueTranslation(myValueTranslate));
		PAssert.thatSingleton(
				cassandraData.apply("Count",
						Count.<KV<Long, String>> globally())).isEqualTo(1L);

		pipeline.run();
	}

	/**
	 * Returns configuration of CqlInutFormat. Mandatory parameters required
	 * apart from inputformat class name, key class, value class are thrift
	 * port, thrift address, partitioner class, keyspace and columnfamily name
	 * 
	 */
	public static Configuration getConfiguration(HIFTestOptions options) {
		Configuration conf = new Configuration();
		conf.set("cassandra.input.thrift.port",
				String.format("%d", options.getServerPort()));
		conf.set("cassandra.input.thrift.address", options.getServerIp());
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
		conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
		conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
				org.apache.cassandra.hadoop.cql3.CqlInputFormat.class,
				InputFormat.class);
		conf.setClass(HadoopInputFormatIOContants.KEY_CLASS,
				java.lang.Long.class, Object.class);
		conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS,
				com.datastax.driver.core.Row.class, Object.class);
		return conf;
	}
}
