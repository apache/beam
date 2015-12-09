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
import org.apache.flink.core.memory.DataInputView;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class StateCheckpointReader {

	private final DataInputView input;

	public StateCheckpointReader(DataInputView in) {
		this.input = in;
	}

	public ByteString getTag() throws IOException {
		return ByteString.copyFrom(readRawData());
	}

	public String getTagToString() throws IOException {
		return input.readUTF();
	}

	public ByteString getData() throws IOException {
		return ByteString.copyFrom(readRawData());
	}

	public int getInt() throws IOException {
		validate();
		return input.readInt();
	}

	public byte getByte() throws IOException {
		validate();
		return input.readByte();
	}

	public Instant getTimestamp() throws IOException {
		validate();
		Long watermarkMillis = input.readLong();
		return new Instant(TimeUnit.MICROSECONDS.toMillis(watermarkMillis));
	}

	public <K> K deserializeKey(CoderTypeSerializer<K> keySerializer) throws IOException {
		return deserializeObject(keySerializer);
	}

	public <T> T deserializeObject(CoderTypeSerializer<T> objectSerializer) throws IOException {
		return objectSerializer.deserialize(input);
	}

	/////////			Helper Methods			///////

	private byte[] readRawData() throws IOException {
		validate();
		int size = input.readInt();

		byte[] serData = new byte[size];
		int bytesRead = input.read(serData);
		if (bytesRead != size) {
			throw new RuntimeException("Error while deserializing checkpoint. Not enough bytes in the input stream.");
		}
		return serData;
	}

	private void validate() {
		if (this.input == null) {
			throw new RuntimeException("StateBackend not initialized yet.");
		}
	}
}
