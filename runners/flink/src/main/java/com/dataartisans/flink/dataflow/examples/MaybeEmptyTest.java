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
