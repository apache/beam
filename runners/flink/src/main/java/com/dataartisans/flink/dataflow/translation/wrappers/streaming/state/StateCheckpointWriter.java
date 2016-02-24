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
import com.google.protobuf.ByteString;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class StateCheckpointWriter {

	private final AbstractStateBackend.CheckpointStateOutputView output;

	public static StateCheckpointWriter create(AbstractStateBackend.CheckpointStateOutputView output) {
		return new StateCheckpointWriter(output);
	}

	private StateCheckpointWriter(AbstractStateBackend.CheckpointStateOutputView output) {
		this.output = output;
	}

	/////////			Creating the serialized versions of the different types of state held by dataflow			///////

	public StateCheckpointWriter addValueBuilder() throws IOException {
		validate();
		StateType.serialize(StateType.VALUE, this);
		return this;
	}

	public StateCheckpointWriter addWatermarkHoldsBuilder() throws IOException {
		validate();
		StateType.serialize(StateType.WATERMARK, this);
		return this;
	}

	public StateCheckpointWriter addListUpdatesBuilder() throws IOException {
		validate();
		StateType.serialize(StateType.LIST, this);
		return this;
	}

	public StateCheckpointWriter addAccumulatorBuilder() throws IOException {
		validate();
		StateType.serialize(StateType.ACCUMULATOR, this);
		return this;
	}

	/////////			Setting the tag for a given state element			///////

	public StateCheckpointWriter setTag(ByteString stateKey) throws IOException {
		return writeData(stateKey.toByteArray());
	}

	public StateCheckpointWriter setTag(String stateKey) throws IOException {
		output.writeUTF(stateKey);
		return this;
	}


	public <K> StateCheckpointWriter serializeKey(K key, CoderTypeSerializer<K> keySerializer) throws IOException {
		return serializeObject(key, keySerializer);
	}

	public <T> StateCheckpointWriter serializeObject(T object, CoderTypeSerializer<T> objectSerializer) throws IOException {
		objectSerializer.serialize(object, output);
		return this;
	}

	/////////			Write the actual serialized data			//////////

	public StateCheckpointWriter setData(ByteString data) throws IOException {
		return writeData(data.toByteArray());
	}

	public StateCheckpointWriter setData(byte[] data) throws IOException {
		return writeData(data);
	}

	public StateCheckpointWriter setTimestamp(Instant timestamp) throws IOException {
		validate();
		output.writeLong(TimeUnit.MILLISECONDS.toMicros(timestamp.getMillis()));
		return this;
	}

	public StateCheckpointWriter writeInt(int number) throws IOException {
		validate();
		output.writeInt(number);
		return this;
	}

	public StateCheckpointWriter writeByte(byte b) throws IOException {
		validate();
		output.writeByte(b);
		return this;
	}

	/////////			Helper Methods			///////

	private StateCheckpointWriter writeData(byte[] data) throws IOException {
		validate();
		output.writeInt(data.length);
		output.write(data);
		return this;
	}

	private void validate() {
		if (this.output == null) {
			throw new RuntimeException("StateBackend not initialized yet.");
		}
	}
}