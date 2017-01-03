/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

import com.datastax.driver.core.Row;

public class HIFWithCassandraTest implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Test
	public void testHadoopInputFormatSource() throws IOException {

		
		Pipeline p = TestPipeline.create();
		Configuration conf = new Configuration();

		String KEYSPACE = "mobile_data_usage";

		String COLUMN_FAMILY = "subscriber";

		conf.set("cassandra.input.thrift.port", "9160");

		conf.set("cassandra.input.thrift.address", "127.0.0.1");// 10.51.234.135

		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");

		conf.set("cassandra.input.keyspace", KEYSPACE);

		conf.set("cassandra.input.columnfamily", COLUMN_FAMILY);

		TypeDescriptor<org.apache.cassandra.hadoop.cql3.CqlInputFormat> inputFormatTypeDesc = new TypeDescriptor<org.apache.cassandra.hadoop.cql3.CqlInputFormat>() {
		};
		conf.setClass("mapreduce.job.inputformat.class",
				inputFormatTypeDesc.getRawType(), InputFormat.class);

		TypeDescriptor<Long> keyTypeDesc = new TypeDescriptor<Long>() {
		};

		conf.setClass("key.class", keyTypeDesc.getRawType(), Object.class);

		TypeDescriptor<com.datastax.driver.core.Row> valueTypeDesc = new TypeDescriptor<com.datastax.driver.core.Row>() {
		};

		conf.setClass("value.class", valueTypeDesc.getRawType(), Object.class);

		SimpleFunction<Row, MyCassandraRow> myValueTranslate = new SimpleFunction<Row, MyCassandraRow>() {

			@Override
			public MyCassandraRow apply(Row input) {
				return new MyCassandraRow(input.getString("subscriber_email"));
			}
		};

		POutput cassandraData = p.apply(HadoopInputFormatIO
				.<Long, MyCassandraRow> read().withConfiguration(conf)
				.withValueTranslation(myValueTranslate));
		/*
		 * PCollection<Long> apply = cassandraData.apply(Count.<KV<Long,
		 * MyCassandraRow>>globally());
		 * PAssert.thatSingleton(apply).isEqualTo((long) 3);
		 */
		p.run().waitUntilFinish();
	}

	@DefaultCoder(AvroCoder.class)
	class MyCassandraRow implements Serializable {
		private String subscriberEmail;

		public MyCassandraRow(String email) {
			this.subscriberEmail = email;
		}

		public String getSubscriberEmail() {
			return subscriberEmail;
		}

		public void setSubscriberEmail(String subscriberEmail) {
			this.subscriberEmail = subscriberEmail;
		}

	}
}
