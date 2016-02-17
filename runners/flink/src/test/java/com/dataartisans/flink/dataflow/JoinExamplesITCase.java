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

import com.dataartisans.flink.dataflow.util.JoinExamples;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.Arrays;
import java.util.List;


/**
 * Unfortunately we need to copy the code from the Dataflow SDK because it is not public there.
 */
public class JoinExamplesITCase extends JavaProgramTestBase {

	protected String resultPath;

	public JoinExamplesITCase(){
	}

	private static final TableRow row1 = new TableRow()
			.set("ActionGeo_CountryCode", "VM").set("SQLDATE", "20141212")
			.set("Actor1Name", "BANGKOK").set("SOURCEURL", "http://cnn.com");
	private static final TableRow row2 = new TableRow()
			.set("ActionGeo_CountryCode", "VM").set("SQLDATE", "20141212")
			.set("Actor1Name", "LAOS").set("SOURCEURL", "http://www.chicagotribune.com");
	private static final TableRow row3 = new TableRow()
			.set("ActionGeo_CountryCode", "BE").set("SQLDATE", "20141213")
			.set("Actor1Name", "AFGHANISTAN").set("SOURCEURL", "http://cnn.com");
	static final TableRow[] EVENTS = new TableRow[] {
			row1, row2, row3
	};
	static final List<TableRow> EVENT_ARRAY = Arrays.asList(EVENTS);

	private static final TableRow cc1 = new TableRow()
			.set("FIPSCC", "VM").set("HumanName", "Vietnam");
	private static final TableRow cc2 = new TableRow()
			.set("FIPSCC", "BE").set("HumanName", "Belgium");
	static final TableRow[] CCS = new TableRow[] {
			cc1, cc2
	};
	static final List<TableRow> CC_ARRAY = Arrays.asList(CCS);

	static final String[] JOINED_EVENTS = new String[] {
			"Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: LAOS, "
					+ "url: http://www.chicagotribune.com",
			"Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: BANGKOK, "
					+ "url: http://cnn.com",
			"Country code: BE, Country name: Belgium, Event info: Date: 20141213, Actor1: AFGHANISTAN, "
					+ "url: http://cnn.com"
	};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(JOINED_EVENTS), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {

		Pipeline p = FlinkTestPipeline.create();

		PCollection<TableRow> input1 = p.apply(Create.of(EVENT_ARRAY));
		PCollection<TableRow> input2 = p.apply(Create.of(CC_ARRAY));

		PCollection<String> output = JoinExamples.joinEvents(input1, input2);

		output.apply(TextIO.Write.to(resultPath));

		p.run();
	}
}

