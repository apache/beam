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
package com.dataartisans.flink.dataflow.translation.types;

import com.dataartisans.flink.dataflow.translation.wrappers.DataInputViewWrapper;
import com.dataartisans.flink.dataflow.translation.wrappers.DataOutputViewWrapper;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s
 */
public class CoderTypeSerializer<T> extends TypeSerializer<T> {
	
	private Coder<T> coder;
	private transient DataInputViewWrapper inputWrapper;
	private transient DataOutputViewWrapper outputWrapper;

	// We use this for internal encoding/decoding for creating copies using the Coder.
	private transient InspectableByteArrayOutputStream buffer;

	public CoderTypeSerializer(Coder<T> coder) {
		this.coder = coder;
		this.inputWrapper = new DataInputViewWrapper(null);
		this.outputWrapper = new DataOutputViewWrapper(null);

		buffer = new InspectableByteArrayOutputStream();
	}
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.inputWrapper = new DataInputViewWrapper(null);
		this.outputWrapper = new DataOutputViewWrapper(null);

		buffer = new InspectableByteArrayOutputStream();
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public CoderTypeSerializer<T> duplicate() {
		return new CoderTypeSerializer<>(coder);
	}

	@Override
	public T createInstance() {
		return null;
	}

	@Override
	public T copy(T t) {
		buffer.reset();
		try {
			coder.encode(t, buffer, Coder.Context.OUTER);
		} catch (IOException e) {
			throw new RuntimeException("Could not copy.", e);
		}
		try {
			return coder.decode(new ByteArrayInputStream(buffer.getBuffer(), 0, buffer
					.size()), Coder.Context.OUTER);
		} catch (IOException e) {
			throw new RuntimeException("Could not copy.", e);
		}
	}

	@Override
	public T copy(T t, T reuse) {
		return copy(t);
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(T t, DataOutputView dataOutputView) throws IOException {
		outputWrapper.setOutputView(dataOutputView);
		coder.encode(t, outputWrapper, Coder.Context.NESTED);
	}

	@Override
	public T deserialize(DataInputView dataInputView) throws IOException {
		try {
			inputWrapper.setInputView(dataInputView);
			return coder.decode(inputWrapper, Coder.Context.NESTED);
		} catch (CoderException e) {
			Throwable cause = e.getCause();
			if (cause instanceof EOFException) {
				throw (EOFException) cause;
			} else {
				throw e;
			}
		}
	}

	@Override
	public T deserialize(T t, DataInputView dataInputView) throws IOException {
		return deserialize(dataInputView);
	}

	@Override
	public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
		serialize(deserialize(dataInputView), dataOutputView);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CoderTypeSerializer that = (CoderTypeSerializer) o;

		if (!coder.equals(that.coder)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return coder.hashCode();
	}
}
