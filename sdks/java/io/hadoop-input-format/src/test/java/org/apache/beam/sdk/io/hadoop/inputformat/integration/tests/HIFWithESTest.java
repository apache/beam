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

import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.junit.Test;

public class HIFWithESTest implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Test
	public void test() throws IOException {
		TestPipeline p = TestPipeline.create();
		Configuration conf = new Configuration();

		conf.set(ConfigurationOptions.ES_NODES, "10.51.234.135:9200");
		conf.set("es.resource", "/my_data/logs");

		Class inputFormatClassName;
		inputFormatClassName = org.elasticsearch.hadoop.mr.EsInputFormat.class;

		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName,
				InputFormat.class);

		Class keyClass = Text.class;

		conf.setClass("key.class", keyClass, Object.class);

		Class valueClass = MapWritable.class;

		conf.setClass("value.class", valueClass, Object.class);
		conf.setClass("mapred.mapoutput.value.class", valueClass, Object.class);

		SimpleFunction<MapWritable, String> myValueTranslate = new SimpleFunction<MapWritable, String>() {

			@Override
			public String apply(MapWritable input) {
				return input.toString();
			}
		};
		PCollection<KV<Text, MapWritable>> esData = p.apply(HadoopInputFormatIO
				.<Text, MapWritable> read().withConfiguration(conf));
		/*
		 * PCollection<Long> apply = esData.apply(Count.<KV<Text,
		 * MapWritable>>globally());
		 * PAssert.thatSingleton(apply).isEqualTo((long) 1001);
		 */
		p.run().waitUntilFinish();
	}

}
