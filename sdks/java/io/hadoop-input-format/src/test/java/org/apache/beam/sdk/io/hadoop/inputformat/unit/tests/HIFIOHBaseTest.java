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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;


import java.io.Serializable;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
/**
 * Runs test to validate HadoopInputFromatIO for a HBase instance on GCP.
 *
 * You need to pass HBase server IP and port in beamTestPipelineOptions
 *
 * <p>
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]'
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOHBaseTest implements Serializable {
	private static final long serialVersionUID = 1L;

	@Test
	public void testHIFOnHBase() throws Throwable {
		TestPipeline p = TestPipeline.create();
		Configuration conf = getHBaseConfiguration();
		SimpleFunction<Result, String> myValueTranslate = new SimpleFunction<Result, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String apply(Result input) {
				return Bytes.toString(input.getValue(Bytes.toBytes("account"),Bytes.toBytes("name")));
			}
		};

		PCollection<KV<ImmutableBytesWritable, String>> hbaseData =
				p.apply(HadoopInputFormatIO.<ImmutableBytesWritable, String>read().withConfiguration(conf)
						.withValueTranslation(myValueTranslate));

		PAssert.thatSingleton(hbaseData.apply("Count", Count.<KV<ImmutableBytesWritable, String>>globally()))
		.isEqualTo(4L);
		p.run().waitUntilFinish();
	}
	
	public Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "104.154.230.7");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.mapreduce.inputtable", "studentData");
		conf.setClass("mapreduce.job.inputformat.class",
				org.apache.hadoop.hbase.mapreduce.TableInputFormat.class,Object.class);
		conf.setClass("key.class", ImmutableBytesWritable.class, Object.class);
		conf.setClass("value.class", org.apache.hadoop.hbase.client.Result.class, Object.class);
		return conf;
	}
}

