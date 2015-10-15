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
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import org.apache.flink.shaded.com.google.common.base.Preconditions;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ReadSourceITCase extends JavaProgramTestBase {

	protected String resultPath;

	public ReadSourceITCase(){
	}

	static final String[] EXPECTED_RESULT = new String[] {
			"1", "2", "3", "4", "5", "6", "7", "8", "9"};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		runProgram(resultPath);
	}

	private static void runProgram(String resultPath) {

		Pipeline p = FlinkTestPipeline.create();

		PCollection<String> result = p
				.apply(Read.from(new ReadSource(1, 10)))
				.apply(ParDo.of(new DoFn<Integer, String>() {
					@Override
					public void processElement(ProcessContext c) throws Exception {
						c.output(c.element().toString());
					}
				}));

		result.apply(TextIO.Write.to(resultPath));
		p.run();
	}
}

class ReadSource extends BoundedSource<Integer> {
	final int from;
	final int to;

	ReadSource(int from, int to) {
		this.from = from;
		this.to = to;
	}

	@Override
	public List<ReadSource> splitIntoBundles(long desiredShardSizeBytes, PipelineOptions options)
			throws Exception {
		List<ReadSource> res = new ArrayList<>();
		FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
		int numWorkers = flinkOptions.getParallelism();
		Preconditions.checkArgument(numWorkers > 0, "Number of workers should be larger than 0.");

		float step = 1.0f * (to - from) / numWorkers;
		for (int i = 0; i < numWorkers; ++i) {
			res.add(new ReadSource(Math.round(from + i * step), Math.round(from + (i + 1) * step)));
		}
		return res;
	}

	@Override
	public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
		return 8 * (to - from);
	}

	@Override
	public boolean producesSortedKeys(PipelineOptions options) throws Exception {
		return true;
	}

	@Override
	public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
		return new RangeReader(this);
	}

	@Override
	public void validate() {}

	@Override
	public Coder<Integer> getDefaultOutputCoder() {
		return BigEndianIntegerCoder.of();
	}

	private class RangeReader extends BoundedReader<Integer> {
		private int current;

		public RangeReader(ReadSource source) {
			this.current = source.from - 1;
		}

		@Override
		public boolean start() throws IOException {
			return true;
		}

		@Override
		public boolean advance() throws IOException {
			current++;
			return (current < to);
		}

		@Override
		public Integer getCurrent() {
			return current;
		}

		@Override
		public void close() throws IOException {
			// Nothing
		}

		@Override
		public BoundedSource<Integer> getCurrentSource() {
			return ReadSource.this;
		}
	}
}

