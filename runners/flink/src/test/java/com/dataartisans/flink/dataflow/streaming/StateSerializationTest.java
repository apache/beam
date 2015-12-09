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
package com.dataartisans.flink.dataflow.streaming;

import com.dataartisans.flink.dataflow.translation.wrappers.streaming.state.FlinkStateInternals;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.state.StateCheckpointReader;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.state.StateCheckpointUtils;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.state.StateCheckpointWriter;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.state.*;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class StateSerializationTest {

	private static final StateNamespace NAMESPACE_1 = StateNamespaces.global();
	private static final String KEY_PREFIX = "TEST_";

	private static final StateTag<ValueState<String>> STRING_VALUE_ADDR =
			StateTags.value("stringValue", StringUtf8Coder.of());
	private static final StateTag<ValueState<Integer>> INT_VALUE_ADDR =
			StateTags.value("stringValue", VarIntCoder.of());
	private static final StateTag<CombiningValueState<Integer, Integer>> SUM_INTEGER_ADDR =
			StateTags.combiningValueFromInputInternal(
					"sumInteger", VarIntCoder.of(), new Sum.SumIntegerFn());
	private static final StateTag<BagState<String>> STRING_BAG_ADDR =
			StateTags.bag("stringBag", StringUtf8Coder.of());
	private static final StateTag<WatermarkStateInternal> WATERMARK_BAG_ADDR =
			StateTags.watermarkStateInternal("watermark");

	private Combine.CombineFn combiner = new Sum.SumIntegerFn();

	private Map<String, FlinkStateInternals<String>> statePerKey = new HashMap<>();

	private Map<String, Set<TimerInternals.TimerData>> activeTimers = new HashMap<>();

	private void initializeStateAndTimers() throws CannotProvideCoderException {
		for (int i = 0; i < 10; i++) {
			String key = KEY_PREFIX + i;

			FlinkStateInternals state = initializeStateForKey(key);
			Set<TimerInternals.TimerData> timers = new HashSet<>();
			for (int j = 0; j < 5; j++) {
				TimerInternals.TimerData timer = TimerInternals
						.TimerData.of(NAMESPACE_1,
								new Instant(1000 + i + j), TimeDomain.values()[j % 3]);
				timers.add(timer);
			}

			statePerKey.put(key, state);
			activeTimers.put(key, timers);
		}
	}

	private FlinkStateInternals<String> initializeStateForKey(String key) throws CannotProvideCoderException {
		FlinkStateInternals<String> state = createState(key);

		ValueState<String> value = state.state(NAMESPACE_1, STRING_VALUE_ADDR);
		value.set("test");

		ValueState<Integer> value2 = state.state(NAMESPACE_1, INT_VALUE_ADDR);
		value2.set(4);
		value2.set(5);

		CombiningValueState<Integer, Integer> combiningValue = state.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		combiningValue.add(1);
		combiningValue.add(2);

		WatermarkStateInternal watermark = state.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		watermark.add(new Instant(1000));

		BagState<String> bag = state.state(NAMESPACE_1, STRING_BAG_ADDR);
		bag.add("v1");
		bag.add("v2");
		bag.add("v3");
		bag.add("v4");
		return state;
	}

	private boolean restoreAndTestState(DataInputView in) throws Exception {
		StateCheckpointReader reader = new StateCheckpointReader(in);
		final ClassLoader userClassloader = this.getClass().getClassLoader();
		Coder<? extends BoundedWindow> windowCoder = IntervalWindow.getCoder();
		Coder<String> keyCoder = StringUtf8Coder.of();

		boolean comparisonRes = true;

		for(String key: statePerKey.keySet()) {
			comparisonRes &= checkStateForKey(key);
		}

		// restore the timers
		Map<String, Set<TimerInternals.TimerData>> restoredTimersPerKey = StateCheckpointUtils.decodeTimers(reader, windowCoder, keyCoder);
		if(activeTimers.size() != restoredTimersPerKey.size()) {
			return false;
		}

		for(String key: statePerKey.keySet()) {
			Set<TimerInternals.TimerData> originalTimers = activeTimers.get(key);
			Set<TimerInternals.TimerData> restoredTimers = restoredTimersPerKey.get(key);
			comparisonRes &= checkTimersForKey(originalTimers, restoredTimers);
		}

		// restore the state
		Map<String, FlinkStateInternals<String>> restoredPerKeyState = StateCheckpointUtils.decodeState(reader, combiner.asKeyedFn(), keyCoder, windowCoder, userClassloader);
		if(restoredPerKeyState.size() != statePerKey.size()) {
			return false;
		}

		for(String key: statePerKey.keySet()) {
			FlinkStateInternals<String> originalState = statePerKey.get(key);
			FlinkStateInternals<String> restoredState = restoredPerKeyState.get(key);
			comparisonRes &= checkStateForKey(originalState, restoredState);
		}
		return comparisonRes;
	}

	private boolean checkStateForKey(String key) throws CannotProvideCoderException {
		FlinkStateInternals<String> state = statePerKey.get(key);

		ValueState<String> value = state.state(NAMESPACE_1, STRING_VALUE_ADDR);
		boolean comp = value.get().read().equals("test");

		ValueState<Integer> value2 = state.state(NAMESPACE_1, INT_VALUE_ADDR);
		comp &= value2.get().read().equals(5);

		CombiningValueState<Integer, Integer> combiningValue = state.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		comp &= combiningValue.get().read().equals(3);

		WatermarkStateInternal watermark = state.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		comp &= watermark.get().read().equals(new Instant(1000));

		BagState<String> bag = state.state(NAMESPACE_1, STRING_BAG_ADDR);
		Iterator<String> it = bag.get().read().iterator();
		int i = 0;
		while(it.hasNext()) {
			comp &= it.next().equals("v"+ (++i));
		}
		return comp;
	}

	private void storeState(StateBackend.CheckpointStateOutputView out) throws Exception {
		StateCheckpointWriter checkpointBuilder = StateCheckpointWriter.create(out);
		Coder<String> keyCoder = StringUtf8Coder.of();

		// checkpoint the timers
		StateCheckpointUtils.encodeTimers(activeTimers, checkpointBuilder,keyCoder);

		// checkpoint the state
		StateCheckpointUtils.encodeState(statePerKey, checkpointBuilder, keyCoder);
	}

	private boolean checkTimersForKey(Set<TimerInternals.TimerData> originalTimers, Set<TimerInternals.TimerData> restoredTimers) {
		boolean comp = true;
		if(restoredTimers == null) {
			return false;
		}

		if(originalTimers.size() != restoredTimers.size()) {
			return false;
		}

		for(TimerInternals.TimerData timer: originalTimers) {
			comp &= restoredTimers.contains(timer);
		}
		return comp;
	}

	private boolean checkStateForKey(FlinkStateInternals<String> originalState, FlinkStateInternals<String> restoredState) throws CannotProvideCoderException {
		if(restoredState == null) {
			return false;
		}

		ValueState<String> orValue = originalState.state(NAMESPACE_1, STRING_VALUE_ADDR);
		ValueState<String> resValue = restoredState.state(NAMESPACE_1, STRING_VALUE_ADDR);
		boolean comp = orValue.get().read().equals(resValue.get().read());

		ValueState<Integer> orIntValue = originalState.state(NAMESPACE_1, INT_VALUE_ADDR);
		ValueState<Integer> resIntValue = restoredState.state(NAMESPACE_1, INT_VALUE_ADDR);
		comp &= orIntValue.get().read().equals(resIntValue.get().read());

		CombiningValueState<Integer, Integer> combOrValue = originalState.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		CombiningValueState<Integer, Integer> combResValue = restoredState.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		comp &= combOrValue.get().read().equals(combResValue.get().read());

		WatermarkStateInternal orWatermark = originalState.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		WatermarkStateInternal resWatermark = restoredState.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		comp &= orWatermark.get().read().equals(resWatermark.get().read());

		BagState<String> orBag = originalState.state(NAMESPACE_1, STRING_BAG_ADDR);
		BagState<String> resBag = restoredState.state(NAMESPACE_1, STRING_BAG_ADDR);

		Iterator<String> orIt = orBag.get().read().iterator();
		Iterator<String> resIt = resBag.get().read().iterator();

		while (orIt.hasNext() && resIt.hasNext()) {
			comp &= orIt.next().equals(resIt.next());
		}

		return !((orIt.hasNext() && !resIt.hasNext()) || (!orIt.hasNext() && resIt.hasNext())) && comp;
	}

	private FlinkStateInternals<String> createState(String key) throws CannotProvideCoderException {
		return new FlinkStateInternals<>(
				key,
				StringUtf8Coder.of(),
				IntervalWindow.getCoder(),
				combiner.asKeyedFn());
	}

	@Test
	public void test() throws Exception {
		StateSerializationTest test = new StateSerializationTest();
		test.initializeStateAndTimers();

		MemoryStateBackend.MemoryCheckpointOutputStream memBackend = new MemoryStateBackend.MemoryCheckpointOutputStream(25728);
		StateBackend.CheckpointStateOutputView out = new StateBackend.CheckpointStateOutputView(memBackend);

		test.storeState(out);

		byte[] contents = memBackend.closeAndGetBytes();
		ByteArrayInputView in = new ByteArrayInputView(contents);

		assertEquals(test.restoreAndTestState(in), true);
	}

}
