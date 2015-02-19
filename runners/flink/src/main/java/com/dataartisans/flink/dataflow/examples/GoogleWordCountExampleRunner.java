package com.dataartisans.flink.dataflow.examples;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount.CountWords;
import com.google.cloud.dataflow.examples.WordCount.Options;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public class GoogleWordCountExampleRunner {
	
	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.create().as(Options.class);
		options.setOutput("/tmp/output2.txt");
		options.setInput("/tmp/documents/hello_world.txt");
		//options.setRunner(DirectPipelineRunner.class);
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
