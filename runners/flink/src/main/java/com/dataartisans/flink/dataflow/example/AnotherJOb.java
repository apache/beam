package com.dataartisans.flink.dataflow.example;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * Created by max on 16/02/15.
 */
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
