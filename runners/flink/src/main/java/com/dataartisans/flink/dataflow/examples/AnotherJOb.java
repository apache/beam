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
package com.dataartisans.flink.dataflow.examples;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

public class AnotherJOb {
	public static void main(String[] args) {

		WordCount.Options options = PipelineOptionsFactory.create().as(WordCount.Options.class);
		options.setOutput("/tmp/output2.txt");
		options.setInput("/Users/max/hello_world.txt");
		//options.setRunner(DirectPipelineRunner.class);
		options.setRunner(FlinkPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(ParDo.of(new DoFn<String, String>() {

					@Override
					public void processElement(ProcessContext c) throws Exception {
						c.output(c.element());
					}
				}))
				.apply(TextIO.Write.named("WriteCounts")
						.to(options.getOutput())
						.withNumShards(options.getNumShards()));

		p.run();
	}
}
