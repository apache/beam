/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.streaming;

import com.dataartisans.flink.dataflow.FlinkTestPipeline;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Arrays;

public class GroupByNullKeyTest extends StreamingProgramTestBase implements Serializable {


	protected String resultPath;

	static final String[] EXPECTED_RESULT = new String[] {
			"k: null v: user1 user1 user1 user2 user2 user2 user2 user3"
	};

	public GroupByNullKeyTest(){
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
	}

	public static class ExtractUserAndTimestamp extends DoFn<KV<Integer, String>, String> {
		private static final long serialVersionUID = 0;

		@Override
		public void processElement(ProcessContext c) {
			KV<Integer, String> record = c.element();
			long now = System.currentTimeMillis();
			int timestamp = record.getKey();
			String userName = record.getValue();
			if (userName != null) {
				// Sets the implicit timestamp field to be used in windowing.
				c.outputWithTimestamp(userName, new Instant(timestamp + now));
			}
		}
	}

	@Override
	protected void testProgram() throws Exception {

		Pipeline p = FlinkTestPipeline.createForStreaming();

		PCollection<String> output =
			p.apply(Create.of(Arrays.asList(
					KV.<Integer, String>of(0, "user1"),
					KV.<Integer, String>of(1, "user1"),
					KV.<Integer, String>of(2, "user1"),
					KV.<Integer, String>of(10, "user2"),
					KV.<Integer, String>of(1, "user2"),
					KV.<Integer, String>of(15000, "user2"),
					KV.<Integer, String>of(12000, "user2"),
					KV.<Integer, String>of(25000, "user3"))))
					.apply(ParDo.of(new ExtractUserAndTimestamp()))
					.apply(Window.<String>into(FixedWindows.of(Duration.standardHours(1)))
							.triggering(AfterWatermark.pastEndOfWindow())
							.withAllowedLateness(Duration.ZERO)
							.discardingFiredPanes())

					.apply(ParDo.of(new DoFn<String, KV<Void, String>>() {
						@Override
						public void processElement(ProcessContext c) throws Exception {
							String elem = c.element();
							c.output(KV.<Void, String>of((Void) null, elem));
						}
					}))
					.apply(GroupByKey.<Void, String>create())
					.apply(ParDo.of(new DoFn<KV<Void, Iterable<String>>, String>() {
						@Override
						public void processElement(ProcessContext c) throws Exception {
							KV<Void, Iterable<String>> elem = c.element();
							StringBuilder str = new StringBuilder();
							str.append("k: " + elem.getKey() + " v:");
							for (String v : elem.getValue()) {
								str.append(" " + v);
							}
							c.output(str.toString());
						}
					}));
		output.apply(TextIO.Write.to(resultPath));
		p.run();
	}
}
