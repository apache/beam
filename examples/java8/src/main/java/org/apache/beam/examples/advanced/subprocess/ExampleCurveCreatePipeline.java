/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.advanced.subprocess;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.beam.examples.advanced.subprocess.FinanceExample.Data;
import org.apache.beam.examples.advanced.subprocess.FinanceExample.DataList;
import org.apache.beam.examples.advanced.subprocess.FinanceExample.DataList.Builder;
import org.apache.beam.examples.advanced.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.advanced.subprocess.kernel.SubProcessCommandLineArgs;
import org.apache.beam.examples.advanced.subprocess.kernel.SubProcessCommandLineArgs.Command;
import org.apache.beam.examples.advanced.subprocess.kernel.SubProcessKernel;
import org.apache.beam.examples.advanced.subprocess.utils.CallingSubProcessUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In this example batch pipeline we will invoke a simple Echo C++ library
 * within a DoFn The sample makes use of a ExternalLibraryDoFn class which
 * abstracts the setup and processing of the executable, logs and results. For
 * this example we are using commands passed to the library via ProtoBuffers
 * Please note the c++ library "democurve" is not included with these samples,
 * you can find an example of building the curve in resources such as
 * https://github.com/lballabio/QuantLib/tree/master/Examples/Swap
 *
 */
public class ExampleCurveCreatePipeline {
	static final Logger LOG = LoggerFactory.getLogger(ExampleCurveCreatePipeline.class);

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		SubProcessPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(SubProcessPipelineOptions.class);

		Pipeline p = Pipeline.create(options);
		// Setup the Configuration option used with all transforms
		SubProcessConfiguration configuration = options.getSubProcessConfiguration();

		// Create some sample data to be fed to our c++ library in protobuf
		// format
		List<Integer> sampleData = new ArrayList<>();

		for (int i = 0; i < 1000; i++) {
			sampleData.add(i);
		}

		PCollection<KV<String, String>> results = p.apply(Create.of(sampleData))
				.apply("Generate X 1000 dummy values", 
						ParDo.of(new DoFn<Integer, KV<String, Data>>() {
					@ProcessElement
					public void process(ProcessContext c) {
						// Change the values per curve to produce different
						// numbers in the logs
						Double k = Double.valueOf(c.element()) / 10000;
						Double sign = -1d;

						for (int i = 0; i < 999; i++) {

							// Change the numbers around by adding / subtracting
							// value
							k += i / 10000;
							sign *= -1;
							k *= sign;

							Data sample = FinanceExample.Data.newBuilder()
									.setKey(String.valueOf(c.element()))
									.setD1WRate(0.0382 + k)
									.setD1MRate(0.0372 + k)
									.setD3MRate(0.0363 + k)
									.setD6MRate(0.0353 + k)
									.setD9MRate(0.0348 + k)
									.setD1YRate(0.0345 + k)
									.setS2YRate(0.037125 + k)
									.setS3YRate(0.0398 + k)
									.setS5YRate(0.0443 + k)
									.setS10YRate(0.0516 + k)
									.setS15YRate(0.055175 + k)
									.build();

							// In this step we will batch the data up into
							// blocks of 50. This makes the pipeline
							// have to do a shuffle. This is a good trade off in
							// this example as the curve
							// creation is a very small spin on the CPU the
							// admin cost of spinning up the
							// sub-process becomes a major chunk of the
							// computation time. The batching reduces
							// this by a factor of 50. This step is not needed
							// if the computation would take many
							// seconds as the admin to work ratio would then be
							// small
							c.output(KV.of(String.valueOf((i + (c.element() * 1000)) % 20000), sample));
						}
					}
				})).apply(GroupByKey.<String, Data>create())
				.apply("Create Curve", ParDo.of(new CurveCreateDoFn(configuration, "democurve")));

		// Log every 100000th value
		results.apply("Log every 10000th result", ParDo.of(new DoFn<KV<String, String>, String>() {
			@ProcessElement
			public void process(ProcessContext c) {
				if (Integer.valueOf(c.element().getKey()) % 5000 == 0) {
					LOG.info(c.element().getValue());
				}
			}
		}));
		p.run();
	}

	/**
	 * Wrapper class for the elements that will be sent to the SubProcess.
	 */
	@SuppressWarnings("serial")
	public static class CurveCreateDoFn
	extends DoFn<KV<String, Iterable<Data>>, KV<String, String>> {

		static final Logger LOG = LoggerFactory.getLogger(CurveCreateDoFn.class);

		private SubProcessConfiguration configuration;
		private String binaryName;

		public CurveCreateDoFn(SubProcessConfiguration configuration, String binary) {
			// Pass in configuration information the name of the filename of the
			// sub-process and the level
			// of concurrency
			this.configuration = configuration;
			this.binaryName = binary;
		}

		@Setup
		public void setUp() throws Exception {
			CallingSubProcessUtils.setUp(configuration, binaryName);
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			try {

				// Create the list batch of 50
				Builder batch = DataList.newBuilder();
				for (Data data : c.element().getValue()) {
					batch.addDataList(data);
				}

				LOG.debug("Processing batch of " + batch.getDataListCount());

				String str = Base64.getEncoder()
						.encodeToString(batch.build().toByteArray());

				// Our Library takes a single command in position 0 which it
				// will echo back in the result
				SubProcessCommandLineArgs commands = new SubProcessCommandLineArgs();
				Command command = new Command(0, str);
				commands.putCommand(command);

				// The ProcessingKernel deals with the execution of the process
				SubProcessKernel kernel = new SubProcessKernel(configuration, binaryName);

				// Run the command and work through the results
				List<String> results = kernel.exec(commands);
				for (String s : results) {
					c.output(KV.of(c.element().getKey(), s));
				}
			} catch (Exception ex) {
				LOG.error("Error processing element ", ex);
				throw ex;
			}
		}
	}
}
