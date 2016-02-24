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
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StateCheckpointUtils {

	public static <K> void encodeState(Map<K, FlinkStateInternals<K>> perKeyStateInternals,
							 StateCheckpointWriter writer, Coder<K> keyCoder) throws IOException {
		CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);

		int noOfKeys = perKeyStateInternals.size();
		writer.writeInt(noOfKeys);
		for (Map.Entry<K, FlinkStateInternals<K>> keyStatePair : perKeyStateInternals.entrySet()) {
			K key = keyStatePair.getKey();
			FlinkStateInternals<K> state = keyStatePair.getValue();

			// encode the key
			writer.serializeKey(key, keySerializer);

			// write the associated state
			state.persistState(writer);
		}
	}

	public static <K> Map<K, FlinkStateInternals<K>> decodeState(
			StateCheckpointReader reader,
			OutputTimeFn<? super BoundedWindow> outputTimeFn,
			Coder<K> keyCoder,
			Coder<? extends BoundedWindow> windowCoder,
			ClassLoader classLoader) throws IOException, ClassNotFoundException {

		int noOfKeys = reader.getInt();
		Map<K, FlinkStateInternals<K>> perKeyStateInternals = new HashMap<>(noOfKeys);
		perKeyStateInternals.clear();

		CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);
		for (int i = 0; i < noOfKeys; i++) {

			// decode the key.
			K key = reader.deserializeKey(keySerializer);

			//decode the state associated to the key.
			FlinkStateInternals<K> stateForKey =
					new FlinkStateInternals<>(key, keyCoder, windowCoder, outputTimeFn);
			stateForKey.restoreState(reader, classLoader);
			perKeyStateInternals.put(key, stateForKey);
		}
		return perKeyStateInternals;
	}

	//////////////				Encoding/Decoding the Timers				////////////////


	public static <K> void encodeTimers(Map<K, Set<TimerInternals.TimerData>> allTimers,
							  StateCheckpointWriter writer,
							  Coder<K> keyCoder) throws IOException {
		CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);

		int noOfKeys = allTimers.size();
		writer.writeInt(noOfKeys);
		for (Map.Entry<K, Set<TimerInternals.TimerData>> timersPerKey : allTimers.entrySet()) {
			K key = timersPerKey.getKey();

			// encode the key
			writer.serializeKey(key, keySerializer);

			// write the associated timers
			Set<TimerInternals.TimerData> timers = timersPerKey.getValue();
			encodeTimerDataForKey(writer, timers);
		}
	}

	public static <K> Map<K, Set<TimerInternals.TimerData>> decodeTimers(
			StateCheckpointReader reader,
			Coder<? extends BoundedWindow> windowCoder,
			Coder<K> keyCoder) throws IOException {

		int noOfKeys = reader.getInt();
		Map<K, Set<TimerInternals.TimerData>> activeTimers = new HashMap<>(noOfKeys);
		activeTimers.clear();

		CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);
		for (int i = 0; i < noOfKeys; i++) {

			// decode the key.
			K key = reader.deserializeKey(keySerializer);

			// decode the associated timers.
			Set<TimerInternals.TimerData> timers = decodeTimerDataForKey(reader, windowCoder);
			activeTimers.put(key, timers);
		}
		return activeTimers;
	}

	private static void encodeTimerDataForKey(StateCheckpointWriter writer, Set<TimerInternals.TimerData> timers) throws IOException {
		// encode timers
		writer.writeInt(timers.size());
		for (TimerInternals.TimerData timer : timers) {
			String stringKey = timer.getNamespace().stringKey();

			writer.setTag(stringKey);
			writer.setTimestamp(timer.getTimestamp());
			writer.writeInt(timer.getDomain().ordinal());
		}
	}

	private static Set<TimerInternals.TimerData> decodeTimerDataForKey(
			StateCheckpointReader reader, Coder<? extends BoundedWindow> windowCoder) throws IOException {

		// decode the timers: first their number and then the content itself.
		int noOfTimers = reader.getInt();
		Set<TimerInternals.TimerData> timers = new HashSet<>(noOfTimers);
		for (int i = 0; i < noOfTimers; i++) {
			String stringKey = reader.getTagToString();
			Instant instant = reader.getTimestamp();
			TimeDomain domain = TimeDomain.values()[reader.getInt()];

			StateNamespace namespace = StateNamespaces.fromString(stringKey, windowCoder);
			timers.add(TimerInternals.TimerData.of(namespace, instant, domain));
		}
		return timers;
	}
}
