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
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.common.base.Joiner;
import org.apache.flink.api.io.avro.example.User;
import org.apache.flink.test.util.JavaProgramTestBase;


public class AvroITCase extends JavaProgramTestBase {

	protected String resultPath;
	protected String tmpPath;

	public AvroITCase(){
	}

	static final String[] EXPECTED_RESULT = new String[] {
			"Joe red 3", "Mary blue 4"};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		tmpPath = getTempDirPath("tmp");

	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		runProgram(tmpPath, resultPath);
	}

	private static void runProgram(String tmpPath, String resultPath) {
		Pipeline p = FlinkTestPipeline.create();

		p.apply(Create.of(new User("Joe", 3, "red"), new User("Mary", 4, "blue")).withCoder(AvroCoder.of(User.class)))
				.apply(AvroIO.Write.to(tmpPath).withSchema(User.class));

		p.run();

		p = FlinkTestPipeline.create();

		p.apply(AvroIO.Read.from(tmpPath).withSchema(User.class))
				.apply(ParDo.of(new DoFn<User, String>() {
					@Override
					public void processElement(ProcessContext c) throws Exception {
						User u = c.element();
						String result = u.getName() + " " + u.getFavoriteColor() + " " + u.getFavoriteNumber();
						c.output(result);
					}
				}))
				.apply(TextIO.Write.to(resultPath));

		p.run();
	}

}

