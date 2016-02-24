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
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCount {

	public static class ExtractWordsFn extends DoFn<String, String> {
		private final Aggregator<Long, Long> emptyLines =
				createAggregator("emptyLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			if (c.element().trim().isEmpty()) {
				emptyLines.addValue(1L);
			}

			// Split the line into words.
			String[] words = c.element().split("[^a-zA-Z']+");

			// Output each word encountered into the output PCollection.
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
		}
	}

	public static class CountWords extends PTransform<PCollection<String>,
                    PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

			// Convert lines of text into individual words.
			PCollection<String> words = lines.apply(
					ParDo.of(new ExtractWordsFn()));

			// Count the number of times each word occurs.
			PCollection<KV<String, Long>> wordCounts =
					words.apply(Count.<String>perElement());

			return wordCounts;
		}
	}

	/** A SimpleFunction that converts a Word and Count into a printable string. */
	public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
		@Override
		public String apply(KV<String, Long> input) {
			return input.getKey() + ": " + input.getValue();
		}
	}

	/**
	 * Options supported by {@link WordCount}.
	 * <p>
	 * Inherits standard configuration options.
	 */
	public interface Options extends PipelineOptions, FlinkPipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
		String getInput();
		void setInput(String value);

		@Description("Path of the file to write to")
		String getOutput();
		void setOutput(String value);
	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(Options.class);
		options.setRunner(FlinkPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(new CountWords())
				.apply(MapElements.via(new FormatAsTextFn()))
				.apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

		p.run();
	}

}
