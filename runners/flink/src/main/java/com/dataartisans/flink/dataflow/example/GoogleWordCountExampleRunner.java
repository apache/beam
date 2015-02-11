package com.dataartisans.flink.dataflow.example;

import com.dataartisans.flink.dataflow.FlinkLocalPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount.CountWords;
import com.google.cloud.dataflow.examples.WordCount.Options;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public class GoogleWordCountExampleRunner {
	
	public static void main(String[] args) {
		
		String[] arguments = {
				String.format("--output=%s/output.txt", System.getProperty("java.io.tmpdir"))
		};
		
		Options options = PipelineOptionsFactory.fromArgs(arguments)
				.withValidation().as(Options.class);
		options.setRunner(FlinkLocalPipelineRunner.class);
		
		Pipeline p = Pipeline.create(options);

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(new CountWords())
				.apply(TextIO.Write.named("WriteCounts")
						.to(options.getOutput())
						.withNumShards(options.getNumShards()));

		p.run();
	}

}
