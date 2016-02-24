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
package com.dataartisans.flink.dataflow.translation.wrappers.streaming.io;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A wrapper translating Flink Sources implementing the {@link RichParallelSourceFunction} interface, into
 * unbounded Beam sources (see {@link UnboundedSource}).
 * */
public class UnboundedFlinkSource<T, C extends UnboundedSource.CheckpointMark> extends UnboundedSource<T, C> {

	private final PipelineOptions options;
	private final RichParallelSourceFunction<T> flinkSource;

	public UnboundedFlinkSource(PipelineOptions pipelineOptions, RichParallelSourceFunction<T> source) {
		if(!pipelineOptions.getRunner().equals(FlinkPipelineRunner.class)) {
			throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
		}
		options = Preconditions.checkNotNull(pipelineOptions);
		flinkSource = Preconditions.checkNotNull(source);
		validate();
	}

	public RichParallelSourceFunction<T> getFlinkSource() {
		return this.flinkSource;
	}

	@Override
	public List<? extends UnboundedSource<T, C>> generateInitialSplits(int desiredNumSplits, PipelineOptions options) throws Exception {
		throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
	}

	@Override
	public UnboundedReader<T> createReader(PipelineOptions options, @Nullable C checkpointMark) {
		throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
	}

	@Nullable
	@Override
	public Coder<C> getCheckpointMarkCoder() {
		throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
	}


	@Override
	public void validate() {
		Preconditions.checkNotNull(options);
		Preconditions.checkNotNull(flinkSource);
		if(!options.getRunner().equals(FlinkPipelineRunner.class)) {
			throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
		}
	}

	@Override
	public Coder<T> getDefaultOutputCoder() {
		throw new RuntimeException("Flink Sources are supported only when running with the FlinkPipelineRunner.");
	}
}
