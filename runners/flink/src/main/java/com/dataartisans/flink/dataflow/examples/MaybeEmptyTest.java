package com.dataartisans.flink.dataflow.examples;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * Created by max on 18/02/15.
 */
public class MaybeEmptyTest {

	private static interface Options extends PipelineOptions {
		@Description("Path to the directory or GCS prefix containing files to read from")
		@Default.String("gs://dataflow-samples/shakespeare/")
		String getInput();
		void setInput(String value);

		@Description("Prefix of output URI to write to")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args ){
		Options options = PipelineOptionsFactory.create().as(Options.class);
		options.setOutput("/tmp/output2.txt");
		options.setInput("/tmp/documents");
		//options.setRunner(DirectPipelineRunner.class);
		options.setRunner(FlinkPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		p.apply(Create.of((Void) null)).setCoder(VoidCoder.of())
				.apply(ParDo.of(
						new DoFn<Void, String>() {
							@Override
							public void processElement(DoFn<Void, String>.ProcessContext c) {
								System.out.println("hello");
								c.output("test");
							}
						})).apply(TextIO.Write.to("bla"));
		p.run();
		
	}
}
