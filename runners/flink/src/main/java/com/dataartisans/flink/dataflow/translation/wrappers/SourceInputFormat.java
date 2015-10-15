/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.wrappers;

import com.dataartisans.flink.dataflow.io.ConsoleIO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * A Flink {@link org.apache.flink.api.common.io.InputFormat} that wraps a
 * Dataflow {@link com.google.cloud.dataflow.sdk.io.Source}.
 */
public class SourceInputFormat<T> implements InputFormat<T, SourceInputSplit<T>> {
	private static final Logger LOG = LoggerFactory.getLogger(SourceInputFormat.class);

	private final BoundedSource<T> initialSource;
	private transient PipelineOptions options;
	private final Coder<T> coder;

	private BoundedSource.BoundedReader<T> reader = null;
	private boolean reachedEnd = true;

	public SourceInputFormat(BoundedSource<T> initialSource, PipelineOptions options, Coder<T> coder) {
		this.initialSource = initialSource;
		this.options = options;
		this.coder = coder;
	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(out, options);
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		ObjectMapper mapper = new ObjectMapper();
		options = mapper.readValue(in, PipelineOptions.class);
	}

	@Override
	public void configure(Configuration configuration) {}

	@Override
	public void open(SourceInputSplit<T> sourceInputSplit) throws IOException {
		reader = ((BoundedSource<T>) sourceInputSplit.getSource()).createReader(options);
		reachedEnd = false;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
		try {
			final long estimatedSize = initialSource.getEstimatedSizeBytes(options);

			return new BaseStatistics() {
				@Override
				public long getTotalInputSize() {
					return estimatedSize;

				}

				@Override
				public long getNumberOfRecords() {
					return BaseStatistics.NUM_RECORDS_UNKNOWN;
				}

				@Override
				public float getAverageRecordWidth() {
					return BaseStatistics.AVG_RECORD_BYTES_UNKNOWN;
				}
			};
		} catch (Exception e) {
			LOG.warn("Could not read Source statistics: {}", e);
		}

		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public SourceInputSplit<T>[] createInputSplits(int numSplits) throws IOException {
		long desiredSizeBytes = 10000;
		try {
			desiredSizeBytes = initialSource.getEstimatedSizeBytes(options) / numSplits;
			List<? extends Source<T>> shards = initialSource.splitIntoBundles(desiredSizeBytes,
					options);
			List<SourceInputSplit<T>> splits = Lists.newArrayList();
			int splitCount = 0;
			for (Source<T> shard: shards) {
				splits.add(new SourceInputSplit<>(shard, splitCount++));
			}
			return splits.toArray(new SourceInputSplit[splits.size()]);
		} catch (Exception e) {
			throw new IOException("Could not create input splits from Source.", e);
		}
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(final SourceInputSplit[] sourceInputSplits) {
		return new InputSplitAssigner() {
			private int index = 0;
			private final SourceInputSplit[] splits = sourceInputSplits;
			@Override
			public InputSplit getNextInputSplit(String host, int taskId) {
				if (index < splits.length) {
					return splits[index++];
				} else {
					return null;
				}
			}
		};
	}


	@Override
	public boolean reachedEnd() throws IOException {
		return reachedEnd;
	}

	@Override
	public T nextRecord(T t) throws IOException {

		reachedEnd = !reader.advance();
		if (!reachedEnd) {
			return reader.getCurrent();
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}
}
