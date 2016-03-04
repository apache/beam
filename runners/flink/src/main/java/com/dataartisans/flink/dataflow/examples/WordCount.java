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

import com.dataartisans.flink.dataflow.FlinkPipelineOptions;
import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount.CountWords;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public class WordCount {

	/**
	 * Options supported by {@link WordCount}.
	 * <p>
	 * Inherits standard configuration options.
	 */
	public static interface Options extends PipelineOptions, FlinkPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
		String getInput();
		void setInput(String value);

		@Description("Path of the file to write to")
		String getOutput();
		void setOutput(String value);

		/**
		 * By default (numShards == 0), the system will choose the shard count.
		 * Most programs will not need this option.
		 */
		@Description("Number of output shards (0 if the system should choose automatically)")
		int getNumShards();
		void setNumShards(int value);
	}
	
	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
		options.setRunner(FlinkPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(new CountWords())
				.apply(TextIO.Write.named("WriteCounts")
						.to(options.getOutput())
						.withNumShards(options.getNumShards()));

		p.run();
	}

}
