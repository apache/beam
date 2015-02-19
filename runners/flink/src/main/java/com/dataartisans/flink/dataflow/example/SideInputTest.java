package com.dataartisans.flink.dataflow.example;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.io.IOException;
import java.net.URISyntaxException;

public class SideInputTest {
	public static void main(String[] args) throws IOException, URISyntaxException {
		WordCount.Options options = PipelineOptionsFactory.create().as(WordCount.Options.class);
		options.setOutput("/tmp/output2.txt");
		options.setInput("/tmp/documents/hello_world.txt");
		//options.setRunner(DirectPipelineRunner.class);
		options.setRunner(FlinkPipelineRunner.class);

		Pipeline p = Pipeline.create(options);

		final PCollectionView<String, ?> totalDocuments = p
						.apply(Create.of("Hello!"))
						.apply(View.<String>asSingleton());

		p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
				.apply(ParDo.of(new DoFn<String, String>() {

					@Override
					public void processElement(ProcessContext c) throws Exception {
						String s = c.sideInput(totalDocuments);
						System.out.println("side Input:" + s);
						c.output(c.element());
					}
				}).withSideInputs(totalDocuments)).apply(TextIO.Write.to("/tmp/output"));
		
		p.run();
	}
}
