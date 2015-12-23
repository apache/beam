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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Special Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
 * {@link com.google.cloud.dataflow.sdk.coders.VoidCoder}. We need this because Flink does not
 * allow returning {@code null} from an input reader. We return a {@link VoidValue} instead
 * that behaves like a {@code null}, hopefully.
 */
public class VoidCoderTypeSerializer extends TypeSerializer<VoidCoderTypeSerializer.VoidValue> {

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public VoidCoderTypeSerializer duplicate() {
		return this;
	}

	@Override
	public VoidValue createInstance() {
		return VoidValue.INSTANCE;
	}

	@Override
	public VoidValue copy(VoidValue from) {
		return from;
	}

	@Override
	public VoidValue copy(VoidValue from, VoidValue reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(VoidValue record, DataOutputView target) throws IOException {
		target.writeByte(1);
	}

	@Override
	public VoidValue deserialize(DataInputView source) throws IOException {
		source.readByte();
		return VoidValue.INSTANCE;
	}

	@Override
	public VoidValue deserialize(VoidValue reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		source.readByte();
		target.writeByte(1);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VoidCoderTypeSerializer) {
			VoidCoderTypeSerializer other = (VoidCoderTypeSerializer) obj;
			return other.canEqual(this);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof VoidCoderTypeSerializer;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	public static class VoidValue {
		private VoidValue() {}
		
		public static VoidValue INSTANCE = new VoidValue();
	}


}
