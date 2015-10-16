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
package com.dataartisans.flink.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.Serializable;

public class SideInputITCase extends JavaProgramTestBase implements Serializable {

	private static final String expected = "Hello!";

	protected String resultPath;

	@Override
	protected void testProgram() throws Exception {


		Pipeline p = FlinkTestPipeline.create();


		final PCollectionView<String> sidesInput = p
				.apply(Create.of(expected))
				.apply(View.<String>asSingleton());

		p.apply(Create.of("bli"))
				.apply(ParDo.of(new DoFn<String, String>() {
					@Override
					public void processElement(ProcessContext c) throws Exception {
						String s = c.sideInput(sidesInput);
						c.output(s);
					}
				}).withSideInputs(sidesInput)).apply(TextIO.Write.to(resultPath));

		p.run();
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}
