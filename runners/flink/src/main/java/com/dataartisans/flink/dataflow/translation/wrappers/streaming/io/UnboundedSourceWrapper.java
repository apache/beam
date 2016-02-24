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

import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.joda.time.Instant;

/**
 * A wrapper for Beam's unbounded sources. This class wraps around a source implementing the {@link com.google.cloud.dataflow.sdk.io.Read.Unbounded}
 * interface.
 *
 *</p>
 * For now we support non-parallel, not checkpointed sources.
 * */
public class UnboundedSourceWrapper<T> extends RichSourceFunction<WindowedValue<T>> implements Triggerable {

	private final String name;
	private final UnboundedSource.UnboundedReader<T> reader;

	private StreamingRuntimeContext runtime = null;
	private StreamSource.ManualWatermarkContext<WindowedValue<T>> context = null;

	private volatile boolean isRunning = false;

	public UnboundedSourceWrapper(PipelineOptions options, Read.Unbounded<T> transform) {
		this.name = transform.getName();
		this.reader = transform.getSource().createReader(options, null);
	}

	public String getName() {
		return this.name;
	}

	WindowedValue<T> makeWindowedValue(T output, Instant timestamp) {
		if (timestamp == null) {
			timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
		}
		return WindowedValue.of(output, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
	}

	@Override
	public void run(SourceContext<WindowedValue<T>> ctx) throws Exception {
		if (!(ctx instanceof StreamSource.ManualWatermarkContext)) {
			throw new RuntimeException("We assume that all sources in Dataflow are EventTimeSourceFunction. " +
					"Apparently " + this.name + " is not. Probably you should consider writing your own Wrapper for this source.");
		}

		context = (StreamSource.ManualWatermarkContext<WindowedValue<T>>) ctx;
		runtime = (StreamingRuntimeContext) getRuntimeContext();

		this.isRunning = reader.start();
		setNextWatermarkTimer(this.runtime);

		while (isRunning) {

			// get it and its timestamp from the source
			T item = reader.getCurrent();
			Instant timestamp = reader.getCurrentTimestamp();

			// write it to the output collector
			synchronized (ctx.getCheckpointLock()) {
				context.collectWithTimestamp(makeWindowedValue(item, timestamp), timestamp.getMillis());
			}

			// TODO: This will run the loop only until the underlying reader first indicates input has stalled.
			// try to go to the next record
			this.isRunning = reader.advance();
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		if (this.isRunning) {
			synchronized (context.getCheckpointLock()) {
				long watermarkMillis = this.reader.getWatermark().getMillis();
				context.emitWatermark(new Watermark(watermarkMillis));
			}
			setNextWatermarkTimer(this.runtime);
		}
	}

	private void setNextWatermarkTimer(StreamingRuntimeContext runtime) {
		if (this.isRunning) {
			long watermarkInterval =  runtime.getExecutionConfig().getAutoWatermarkInterval();
			long timeToNextWatermark = getTimeToNextWaternark(watermarkInterval);
			runtime.registerTimer(timeToNextWatermark, this);
		}
	}

	private long getTimeToNextWaternark(long watermarkInterval) {
		return System.currentTimeMillis() + watermarkInterval;
	}
}
