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
import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.CombineWithContext;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.state.*;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.joda.time.Instant;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class StateSerializationTest {

	private static final StateNamespace NAMESPACE_1 = StateNamespaces.global();
	private static final String KEY_PREFIX = "TEST_";

	// TODO: This can be replaced with the standard Sum.SumIntererFn once the state no longer needs
	// to create a StateTag at the point of restoring state. Currently StateTags are compared strictly
	// by type and combiners always use KeyedCombineFnWithContext rather than KeyedCombineFn or CombineFn.
	private static CombineWithContext.KeyedCombineFnWithContext<Object, Integer, int[], Integer> SUM_COMBINER =
		new CombineWithContext.KeyedCombineFnWithContext<Object, Integer, int[], Integer>() {
			@Override
			public int[] createAccumulator(Object key, CombineWithContext.Context c) {
				return new int[1];
			}

			@Override
			public int[] addInput(Object key, int[] accumulator, Integer value, CombineWithContext.Context c) {
				accumulator[0] += value;
				return accumulator;
			}

			@Override
			public int[] mergeAccumulators(Object key, Iterable<int[]> accumulators, CombineWithContext.Context c) {
				int[] r = new int[1];
				for (int[] a : accumulators) {
					r[0] += a[0];
				}
				return r;
			}

			@Override
			public Integer extractOutput(Object key, int[] accumulator, CombineWithContext.Context c) {
				return accumulator[0];
			}
		};

	private static Coder<int[]> INT_ACCUM_CODER = DelegateCoder.of(
		VarIntCoder.of(),
		new DelegateCoder.CodingFunction<int[], Integer>() {
			@Override
			public Integer apply(int[] accumulator) {
				return accumulator[0];
			}
		},
		new DelegateCoder.CodingFunction<Integer, int[]>() {
			@Override
			public int[] apply(Integer value) {
				int[] a = new int[1];
				a[0] = value;
				return a;
			}
		});

	private static final StateTag<Object, ValueState<String>> STRING_VALUE_ADDR =
		StateTags.value("stringValue", StringUtf8Coder.of());
	private static final StateTag<Object, ValueState<Integer>> INT_VALUE_ADDR =
		StateTags.value("stringValue", VarIntCoder.of());
	private static final StateTag<Object, AccumulatorCombiningState<Integer, int[], Integer>> SUM_INTEGER_ADDR =
		StateTags.keyedCombiningValueWithContext("sumInteger", INT_ACCUM_CODER, SUM_COMBINER);
	private static final StateTag<Object, BagState<String>> STRING_BAG_ADDR =
		StateTags.bag("stringBag", StringUtf8Coder.of());
	private static final StateTag<Object, WatermarkHoldState<BoundedWindow>> WATERMARK_BAG_ADDR =
		StateTags.watermarkStateInternal("watermark", OutputTimeFns.outputAtEarliestInputTimestamp());

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
		value.write("test");

		ValueState<Integer> value2 = state.state(NAMESPACE_1, INT_VALUE_ADDR);
		value2.write(4);
		value2.write(5);

		AccumulatorCombiningState<Integer, int[], Integer> combiningValue = state.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		combiningValue.add(1);
		combiningValue.add(2);

		WatermarkHoldState<BoundedWindow> watermark = state.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
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

		for (String key : statePerKey.keySet()) {
			comparisonRes &= checkStateForKey(key);
		}

		// restore the timers
		Map<String, Set<TimerInternals.TimerData>> restoredTimersPerKey = StateCheckpointUtils.decodeTimers(reader, windowCoder, keyCoder);
		if (activeTimers.size() != restoredTimersPerKey.size()) {
			return false;
		}

		for (String key : statePerKey.keySet()) {
			Set<TimerInternals.TimerData> originalTimers = activeTimers.get(key);
			Set<TimerInternals.TimerData> restoredTimers = restoredTimersPerKey.get(key);
			comparisonRes &= checkTimersForKey(originalTimers, restoredTimers);
		}

		// restore the state
		Map<String, FlinkStateInternals<String>> restoredPerKeyState =
			StateCheckpointUtils.decodeState(reader, OutputTimeFns.outputAtEarliestInputTimestamp(), keyCoder, windowCoder, userClassloader);
		if (restoredPerKeyState.size() != statePerKey.size()) {
			return false;
		}

		for (String key : statePerKey.keySet()) {
			FlinkStateInternals<String> originalState = statePerKey.get(key);
			FlinkStateInternals<String> restoredState = restoredPerKeyState.get(key);
			comparisonRes &= checkStateForKey(originalState, restoredState);
		}
		return comparisonRes;
	}

	private boolean checkStateForKey(String key) throws CannotProvideCoderException {
		FlinkStateInternals<String> state = statePerKey.get(key);

		ValueState<String> value = state.state(NAMESPACE_1, STRING_VALUE_ADDR);
		boolean comp = value.read().equals("test");

		ValueState<Integer> value2 = state.state(NAMESPACE_1, INT_VALUE_ADDR);
		comp &= value2.read().equals(5);

		AccumulatorCombiningState<Integer, int[], Integer> combiningValue = state.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		comp &= combiningValue.read().equals(3);

		WatermarkHoldState<BoundedWindow> watermark = state.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		comp &= watermark.read().equals(new Instant(1000));

		BagState<String> bag = state.state(NAMESPACE_1, STRING_BAG_ADDR);
		Iterator<String> it = bag.read().iterator();
		int i = 0;
		while (it.hasNext()) {
			comp &= it.next().equals("v" + (++i));
		}
		return comp;
	}

	private void storeState(AbstractStateBackend.CheckpointStateOutputView out) throws Exception {
		StateCheckpointWriter checkpointBuilder = StateCheckpointWriter.create(out);
		Coder<String> keyCoder = StringUtf8Coder.of();

		// checkpoint the timers
		StateCheckpointUtils.encodeTimers(activeTimers, checkpointBuilder, keyCoder);

		// checkpoint the state
		StateCheckpointUtils.encodeState(statePerKey, checkpointBuilder, keyCoder);
	}

	private boolean checkTimersForKey(Set<TimerInternals.TimerData> originalTimers, Set<TimerInternals.TimerData> restoredTimers) {
		boolean comp = true;
		if (restoredTimers == null) {
			return false;
		}

		if (originalTimers.size() != restoredTimers.size()) {
			return false;
		}

		for (TimerInternals.TimerData timer : originalTimers) {
			comp &= restoredTimers.contains(timer);
		}
		return comp;
	}

	private boolean checkStateForKey(FlinkStateInternals<String> originalState, FlinkStateInternals<String> restoredState) throws CannotProvideCoderException {
		if (restoredState == null) {
			return false;
		}

		ValueState<String> orValue = originalState.state(NAMESPACE_1, STRING_VALUE_ADDR);
		ValueState<String> resValue = restoredState.state(NAMESPACE_1, STRING_VALUE_ADDR);
		boolean comp = orValue.read().equals(resValue.read());

		ValueState<Integer> orIntValue = originalState.state(NAMESPACE_1, INT_VALUE_ADDR);
		ValueState<Integer> resIntValue = restoredState.state(NAMESPACE_1, INT_VALUE_ADDR);
		comp &= orIntValue.read().equals(resIntValue.read());

		AccumulatorCombiningState<Integer, int[], Integer> combOrValue = originalState.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		AccumulatorCombiningState<Integer, int[], Integer> combResValue = restoredState.state(NAMESPACE_1, SUM_INTEGER_ADDR);
		comp &= combOrValue.read().equals(combResValue.read());

		WatermarkHoldState orWatermark = originalState.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		WatermarkHoldState resWatermark = restoredState.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
		comp &= orWatermark.read().equals(resWatermark.read());

		BagState<String> orBag = originalState.state(NAMESPACE_1, STRING_BAG_ADDR);
		BagState<String> resBag = restoredState.state(NAMESPACE_1, STRING_BAG_ADDR);

		Iterator<String> orIt = orBag.read().iterator();
		Iterator<String> resIt = resBag.read().iterator();

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
			OutputTimeFns.outputAtEarliestInputTimestamp());
	}

	@Test
	public void test() throws Exception {
		StateSerializationTest test = new StateSerializationTest();
		test.initializeStateAndTimers();

		MemoryStateBackend.MemoryCheckpointOutputStream memBackend = new MemoryStateBackend.MemoryCheckpointOutputStream(32048);
		AbstractStateBackend.CheckpointStateOutputView out = new AbstractStateBackend.CheckpointStateOutputView(memBackend);

		test.storeState(out);

		byte[] contents = memBackend.closeAndGetBytes();
		DataInputView in = new DataInputDeserializer(contents, 0, contents.length);

		assertEquals(test.restoreAndTestState(in), true);
	}

}
