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
package com.dataartisans.flink.dataflow.translation.wrappers.streaming;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.Map;

/**
 * A wrapper for the {@link com.google.cloud.dataflow.sdk.transforms.ParDo.BoundMulti} Beam transformation.
 * */
public class FlinkParDoBoundMultiWrapper<IN, OUT> extends FlinkAbstractParDoWrapper<IN, OUT, RawUnionValue> {

	private final TupleTag<?> mainTag;
	private final Map<TupleTag<?>, Integer> outputLabels;

	public FlinkParDoBoundMultiWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, DoFn<IN, OUT> doFn, TupleTag<?> mainTag, Map<TupleTag<?>, Integer> tagsToLabels) {
		super(options, windowingStrategy, doFn);
		this.mainTag = Preconditions.checkNotNull(mainTag);
		this.outputLabels = Preconditions.checkNotNull(tagsToLabels);
	}

	@Override
	public void outputWithTimestampHelper(WindowedValue<IN> inElement, OUT output, Instant timestamp, Collector<WindowedValue<RawUnionValue>> collector) {
		checkTimestamp(inElement, timestamp);
		Integer index = outputLabels.get(mainTag);
		collector.collect(makeWindowedValue(
				new RawUnionValue(index, output),
				timestamp,
				inElement.getWindows(),
				inElement.getPane()));
	}

	@Override
	public <T> void sideOutputWithTimestampHelper(WindowedValue<IN> inElement, T output, Instant timestamp, Collector<WindowedValue<RawUnionValue>> collector, TupleTag<T> tag) {
		checkTimestamp(inElement, timestamp);
		Integer index = outputLabels.get(tag);
		if (index != null) {
			collector.collect(makeWindowedValue(
					new RawUnionValue(index, output),
					timestamp,
					inElement.getWindows(),
					inElement.getPane()));
		}
	}

	@Override
	public WindowingInternals<IN, OUT> windowingInternalsHelper(WindowedValue<IN> inElement, Collector<WindowedValue<RawUnionValue>> outCollector) {
		throw new RuntimeException("FlinkParDoBoundMultiWrapper is just an internal operator serving as " +
				"an intermediate transformation for the ParDo.BoundMulti translation. windowingInternals() " +
				"is not available in this class.");
	}
}