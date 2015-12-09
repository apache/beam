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
package com.dataartisans.flink.dataflow.translation.wrappers.streaming.state;

import com.dataartisans.flink.dataflow.translation.types.CoderTypeSerializer;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.TimerOrElement;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;

public abstract class AbstractFlinkTimerInternals<K, VIN> implements TimerInternals, Serializable {

	private TimerOrElement<WindowedValue<KV<K, VIN>>> element;

	private Instant currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

	public TimerOrElement<WindowedValue<KV<K, VIN>>> getElement() {
		return this.element;
	}

	public void setElement(TimerOrElement<WindowedValue<KV<K, VIN>>> value) {
		this.element = value;
	}

	public void setCurrentWatermark(Instant watermark) {
		checkIfValidWatermark(watermark);
		this.currentWatermark = watermark;
	}

	private void setCurrentWatermarkAfterRecovery(Instant watermark) {
		if(!currentWatermark.isEqual(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
			throw new RuntimeException("Explicitly setting the watermark is only allowed on " +
					"initialization after recovery from a node failure. Apparently this is not " +
					"the case here as the watermark is already set.");
		}
		this.currentWatermark = watermark;
	}

	@Override
	public void setTimer(com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData timerKey) {
		K key = element.isTimer() ? (K) element.key() : element.element().getValue().getKey();
		registerTimer(key, timerKey);
	}

	protected abstract void registerTimer(K key, TimerData timerKey);

	@Override
	public void deleteTimer(com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData timerKey) {
		K key = element.isTimer() ? (K) element.key() : element.element().getValue().getKey();
		unregisterTimer(key, timerKey);
	}

	protected abstract void unregisterTimer(K key, TimerData timerKey);

	@Override
	public Instant currentProcessingTime() {
		return Instant.now();
	}

	@Override
	public Instant currentWatermarkTime() {
		return this.currentWatermark;
	}

	private void checkIfValidWatermark(Instant newWatermark) {
		if (currentWatermark.isAfter(newWatermark)) {
			throw new IllegalArgumentException(String.format(
					"Cannot set current watermark to %s. Newer watermarks " +
							"must be no earlier than the current one (%s).",
					newWatermark, this.currentWatermark));
		}
	}

	public void encodeTimerInternals(DoFn.ProcessContext context,
									  StateCheckpointWriter writer,
									  KvCoder<K, VIN> kvCoder,
									  Coder<? extends BoundedWindow> windowCoder) throws IOException {
		if (context == null) {
			throw new RuntimeException("The Context has not been initialized.");
		}

		if (element != null && !element.isTimer()) {
			// create the element coder
			WindowedValue.WindowedValueCoder<KV<K, VIN>> elementCoder = WindowedValue
					.getFullCoder(kvCoder, windowCoder);

			CoderTypeSerializer<WindowedValue<KV<K, VIN>>> serializer =
					new CoderTypeSerializer<>(elementCoder);

			writer.writeByte((byte) 1);
			writer.serializeObject(element.element(), serializer);
		} else {
			// just setting a flag to 0, meaning that there is no value.
			writer.writeByte((byte) 0);
		}
		writer.setTimestamp(currentWatermark);
	}

	public void restoreTimerInternals(StateCheckpointReader reader,
									  KvCoder<K, VIN> kvCoder,
									  Coder<? extends BoundedWindow> windowCoder) throws IOException {

		boolean isSet = (reader.getByte() == (byte) 1);
		if (!isSet) {
			this.element = null;
		} else {
			WindowedValue.WindowedValueCoder<KV<K, VIN>> elementCoder = WindowedValue
					.getFullCoder(kvCoder, windowCoder);

			CoderTypeSerializer<WindowedValue<KV<K, VIN>>> serializer =
					new CoderTypeSerializer<>(elementCoder);

			WindowedValue<KV<K, VIN>> elem = reader.deserializeObject(serializer);
			this.element = TimerOrElement.element(elem);
		}
		setCurrentWatermarkAfterRecovery(reader.getTimestamp());
	}
}
