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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.*;
import com.google.protobuf.ByteString;
import org.apache.flink.util.InstantiationUtil;
import org.joda.time.Instant;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

public class FlinkStateInternals<K> extends MergingStateInternals {

	private final K key;

	private final Coder<K> keyCoder;

	private final Combine.KeyedCombineFn<K, ?, ?, ?> combineFn;

	private final Coder<? extends BoundedWindow> windowCoder;

	private Instant watermarkHoldAccessor;

	public FlinkStateInternals(K key,
							   Coder<K> keyCoder,
							   Coder<? extends BoundedWindow> windowCoder,
							   Combine.KeyedCombineFn<K, ?, ?, ?> combineFn) {
		this.key = key;
		this.combineFn = combineFn;
		this.windowCoder = windowCoder;
		this.keyCoder = keyCoder;
	}

	public Instant getWatermarkHold() {
		return watermarkHoldAccessor;
	}

	/**
	 * This is the interface state has to implement in order for it to be fault tolerant when
	 * executed by the FlinkPipelineRunner.
	 */
	private interface CheckpointableIF {

		boolean shouldPersist();

		void persistState(StateCheckpointWriter checkpointBuilder) throws IOException;
	}

	protected final StateTable inMemoryState = new StateTable() {

		@Override
		protected StateTag.StateBinder binderForNamespace(final StateNamespace namespace) {
			return new StateTag.StateBinder() {

				@Override
				public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
					return new FlinkInMemoryValue<>(encodeKey(namespace, address), coder);
				}

				@Override
				public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
					return new FlinkInMemoryBag<>(encodeKey(namespace, address), elemCoder);
				}

				@Override
				public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT> bindCombiningValue(
						StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
						Coder<AccumT> accumCoder, Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
					return new FlinkInMemoryCombiningValue<>(encodeKey(namespace, address), combineFn, accumCoder);
				}

				@Override
				public <T> WatermarkStateInternal bindWatermark(StateTag<WatermarkStateInternal> address) {
					return new FlinkWatermarkStateInternalImpl(encodeKey(namespace, address));
				}
			};
		}
	};

	@Override
	public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
		return inMemoryState.get(namespace, address);
	}

	public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
		checkpointBuilder.writeInt(getNoOfElements());

		for (State location : inMemoryState.values()) {
			if (!(location instanceof CheckpointableIF)) {
				throw new IllegalStateException(String.format(
						"%s wasn't created by %s -- unable to persist it",
						location.getClass().getSimpleName(),
						getClass().getSimpleName()));
			}
			((CheckpointableIF) location).persistState(checkpointBuilder);
		}
	}

	public void restoreState(StateCheckpointReader checkpointReader, ClassLoader loader)
			throws IOException, ClassNotFoundException {

		// the number of elements to read.
		int noOfElements = checkpointReader.getInt();
		for (int i = 0; i < noOfElements; i++) {
			decodeState(checkpointReader, loader);
		}
	}

	/**
	 * We remove the first character which encodes the type of the stateTag ('s' for system
	 * and 'u' for user). For more details check out the source of
	 * {@link StateTags.StateTagBase#getId()}.
	 */
	private void decodeState(StateCheckpointReader reader, ClassLoader loader)
			throws IOException, ClassNotFoundException {

		StateType stateItemType = StateType.deserialize(reader);
		ByteString stateKey = reader.getTag();

		// first decode the namespace and the tagId...
		String[] namespaceAndTag = stateKey.toStringUtf8().split("\\+");
		if (namespaceAndTag.length != 2) {
			throw new IllegalArgumentException("Invalid stateKey " + stateKey.toString() + ".");
		}
		StateNamespace namespace = StateNamespaces.fromString(namespaceAndTag[0], windowCoder);

		// ... decide if it is a system or user stateTag...
		char ownerTag = namespaceAndTag[1].charAt(0);
		if (ownerTag != 's' && ownerTag != 'u') {
			throw new RuntimeException("Invalid StateTag name.");
		}
		boolean isSystemTag = ownerTag == 's';
		String tagId = namespaceAndTag[1].substring(1);

		// ...then decode the coder (if there is one)...
		Coder coder = null;
		if (!stateItemType.equals(StateType.WATERMARK)) {
			ByteString coderBytes = reader.getData();
			coder = InstantiationUtil.deserializeObject(coderBytes.toByteArray(), loader);
		}

		//... and finally, depending on the type of the state being decoded,
		// 1) create the adequate stateTag,
		// 2) create the state container,
		// 3) restore the actual content.
		switch (stateItemType) {
			case VALUE: {
				StateTag stateTag = StateTags.value(tagId, coder);
				stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
				FlinkInMemoryValue<?> value = (FlinkInMemoryValue<?>) inMemoryState.get(namespace, stateTag);
				value.restoreState(reader);
				break;
			}
			case WATERMARK: {
				StateTag<WatermarkStateInternal> stateTag = StateTags.watermarkStateInternal(tagId);
				stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
				FlinkWatermarkStateInternalImpl watermark = (FlinkWatermarkStateInternalImpl) inMemoryState.get(namespace, stateTag);
				watermark.restoreState(reader);
				break;
			}
			case LIST: {
				StateTag stateTag = StateTags.bag(tagId, coder);
				stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
				FlinkInMemoryBag<?> bag = (FlinkInMemoryBag<?>) inMemoryState.get(namespace, stateTag);
				bag.restoreState(reader);
				break;
			}
			case ACCUMULATOR: {
				StateTag stateTag = StateTags.combiningValue(tagId, coder, combineFn.forKey(this.key, keyCoder));
				stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
				FlinkInMemoryCombiningValue<?, ?, ?> combiningValue = (FlinkInMemoryCombiningValue<?, ?, ?>) inMemoryState.get(namespace, stateTag);
				combiningValue.restoreState(reader);
				break;
			}
			default:
				throw new RuntimeException("Unknown State Type " + stateItemType + ".");
		}
	}

	private ByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
		return ByteString.copyFromUtf8(namespace.stringKey() + "+" + address.getId());
	}

	private int getNoOfElements() {
		int noOfElements = 0;
		for (State state : inMemoryState.values()) {
			if (!(state instanceof CheckpointableIF)) {
				throw new RuntimeException("State Implementations used by the " +
						"Flink Dataflow Runner should implement the CheckpointableIF interface.");
			}

			if (((CheckpointableIF) state).shouldPersist()) {
				noOfElements++;
			}
		}
		return noOfElements;
	}

	private final class FlinkInMemoryValue<T> implements ValueState<T>, CheckpointableIF {

		private final ByteString stateKey;
		private final Coder<T> elemCoder;

		private T value = null;

		public FlinkInMemoryValue(ByteString stateKey, Coder<T> elemCoder) {
			this.stateKey = stateKey;
			this.elemCoder = elemCoder;
		}

		@Override
		public void clear() {
			value = null;
		}

		@Override
		public StateContents<T> get() {
			return new StateContents<T>() {
				@Override
				public T read() {
					return value;
				}
			};
		}

		@Override
		public void set(T input) {
			this.value = input;
		}

		@Override
		public boolean shouldPersist() {
			return value != null;
		}

		@Override
		public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
			if (value != null) {

				// serialize the coder.
				byte[] coder = InstantiationUtil.serializeObject(elemCoder);

				// encode the value into a ByteString
				ByteString.Output stream = ByteString.newOutput();
				elemCoder.encode(value, stream, Coder.Context.OUTER);
				ByteString data = stream.toByteString();

				checkpointBuilder.addValueBuilder()
						.setTag(stateKey)
						.setData(coder)
						.setData(data);
			}
		}

		public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
			ByteString valueContent = checkpointReader.getData();
			T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
			set(outValue);
		}
	}

	private final class FlinkWatermarkStateInternalImpl
			implements WatermarkStateInternal, CheckpointableIF {

		private final ByteString stateKey;

		private Instant minimumHold = null;

		public FlinkWatermarkStateInternalImpl(ByteString stateKey) {
			this.stateKey = stateKey;
		}

		@Override
		public void clear() {
			// Even though we're clearing we can't remove this from the in-memory state map, since
			// other users may already have a handle on this WatermarkBagInternal.
			minimumHold = null;
			watermarkHoldAccessor = null;
		}

		@Override
		public StateContents<Instant> get() {
			return new StateContents<Instant>() {
				@Override
				public Instant read() {
					return minimumHold;
				}
			};
		}

		@Override
		public void add(Instant watermarkHold) {
			if (minimumHold == null || minimumHold.isAfter(watermarkHold)) {
				watermarkHoldAccessor = watermarkHold;
				minimumHold = watermarkHold;
			}
		}

		@Override
		public StateContents<Boolean> isEmpty() {
			return new StateContents<Boolean>() {
				@Override
				public Boolean read() {
					return minimumHold == null;
				}
			};
		}

		@Override
		public String toString() {
			return Objects.toString(minimumHold);
		}

		@Override
		public boolean shouldPersist() {
			return minimumHold != null;
		}

		@Override
		public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
			if (minimumHold != null) {
				checkpointBuilder.addWatermarkHoldsBuilder()
						.setTag(stateKey)
						.setTimestamp(minimumHold);
			}
		}

		public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
			Instant watermark = checkpointReader.getTimestamp();
			add(watermark);
		}
	}

	private final class FlinkInMemoryCombiningValue<InputT, AccumT, OutputT>
			implements CombiningValueStateInternal<InputT, AccumT, OutputT>, CheckpointableIF {

		private final ByteString stateKey;
		private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
		private final Coder<AccumT> accumCoder;

		private AccumT accum;
		private boolean isCleared = true;

		private FlinkInMemoryCombiningValue(ByteString stateKey,
											Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
											Coder<AccumT> accumCoder) {
			Preconditions.checkNotNull(combineFn);
			Preconditions.checkNotNull(accumCoder);

			this.stateKey = stateKey;
			this.combineFn = combineFn;
			this.accumCoder = accumCoder;
			accum = combineFn.createAccumulator();
		}

		@Override
		public void clear() {
			accum = combineFn.createAccumulator();
			isCleared = true;
		}

		@Override
		public StateContents<OutputT> get() {
			return new StateContents<OutputT>() {
				@Override
				public OutputT read() {
					return combineFn.extractOutput(accum);
				}
			};
		}

		@Override
		public void add(InputT input) {
			isCleared = false;
			accum = combineFn.addInput(accum, input);
		}

		@Override
		public StateContents<AccumT> getAccum() {
			return new StateContents<AccumT>() {
				@Override
				public AccumT read() {
					return accum;
				}
			};
		}

		@Override
		public StateContents<Boolean> isEmpty() {
			return new StateContents<Boolean>() {
				@Override
				public Boolean read() {
					return isCleared;
				}
			};
		}

		@Override
		public void addAccum(AccumT accum) {
			isCleared = false;
			this.accum = combineFn.mergeAccumulators(Arrays.asList(this.accum, accum));
		}

		@Override
		public boolean shouldPersist() {
			return accum != null;
		}

		@Override
		public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
			if (accum != null) {

				// serialize the coder.
				byte[] coder = InstantiationUtil.serializeObject(accumCoder);

				// encode the accumulator into a ByteString
				ByteString.Output stream = ByteString.newOutput();
				accumCoder.encode(accum, stream, Coder.Context.OUTER);
				ByteString data = stream.toByteString();

				// put the flag that the next serialized element is an accumulator
				checkpointBuilder.addAccumulatorBuilder()
						.setTag(stateKey)
						.setData(coder)
						.setData(data);
			}
		}

		public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
			ByteString valueContent = checkpointReader.getData();
			AccumT accum = this.accumCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
			addAccum(accum);
		}
	}

	private static final class FlinkInMemoryBag<T> implements BagState<T>, CheckpointableIF {
		private final List<T> contents = new ArrayList<>();

		private final ByteString stateKey;
		private final Coder<T> elemCoder;

		public FlinkInMemoryBag(ByteString stateKey, Coder<T> elemCoder) {
			this.stateKey = stateKey;
			this.elemCoder = elemCoder;
		}

		@Override
		public void clear() {
			contents.clear();
		}

		@Override
		public StateContents<Iterable<T>> get() {
			return new StateContents<Iterable<T>>() {
				@Override
				public Iterable<T> read() {
					return contents;
				}
			};
		}

		@Override
		public void add(T input) {
			contents.add(input);
		}

		@Override
		public StateContents<Boolean> isEmpty() {
			return new StateContents<Boolean>() {
				@Override
				public Boolean read() {
					return contents.isEmpty();
				}
			};
		}

		@Override
		public boolean shouldPersist() {
			return !contents.isEmpty();
		}

		@Override
		public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
			if (!contents.isEmpty()) {
				// serialize the coder.
				byte[] coder = InstantiationUtil.serializeObject(elemCoder);

				checkpointBuilder.addListUpdatesBuilder()
						.setTag(stateKey)
						.setData(coder)
						.writeInt(contents.size());

				for (T item : contents) {
					// encode the element
					ByteString.Output stream = ByteString.newOutput();
					elemCoder.encode(item, stream, Coder.Context.OUTER);
					ByteString data = stream.toByteString();

					// add the data to the checkpoint.
					checkpointBuilder.setData(data);
				}
			}
		}

		public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
			int noOfValues = checkpointReader.getInt();
			for (int j = 0; j < noOfValues; j++) {
				ByteString valueContent = checkpointReader.getData();
				T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
				add(outValue);
			}
		}
	}
}
