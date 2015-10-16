/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.dataartisans.flink.dataflow.translation.functions;


import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.VarInt;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * A UnionCoder encodes RawUnionValues.
 *
 * This file copied from {@link com.google.cloud.dataflow.sdk.transforms.join.UnionCoder}
 */
@SuppressWarnings("serial")
public class UnionCoder extends StandardCoder<RawUnionValue> {
	// TODO: Think about how to integrate this with a schema object (i.e.
	// a tuple of tuple tags).
	/**
	 * Builds a union coder with the given list of element coders.  This list
	 * corresponds to a mapping of union tag to Coder.  Union tags start at 0.
	 */
	public static UnionCoder of(List<Coder<?>> elementCoders) {
		return new UnionCoder(elementCoders);
	}

	@JsonCreator
	public static UnionCoder jsonOf(
			@JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
			List<Coder<?>> elements) {
		return UnionCoder.of(elements);
	}

	private int getIndexForEncoding(RawUnionValue union) {
		if (union == null) {
			throw new IllegalArgumentException("cannot encode a null tagged union");
		}
		int index = union.getUnionTag();
		if (index < 0 || index >= elementCoders.size()) {
			throw new IllegalArgumentException(
					"union value index " + index + " not in range [0.." +
							(elementCoders.size() - 1) + "]");
		}
		return index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void encode(
			RawUnionValue union,
			OutputStream outStream,
			Context context)
			throws IOException, CoderException  {
		int index = getIndexForEncoding(union);
		// Write out the union tag.
		VarInt.encode(index, outStream);

		// Write out the actual value.
		Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
		coder.encode(
				union.getValue(),
				outStream,
				context);
	}

	@Override
	public RawUnionValue decode(InputStream inStream, Context context)
			throws IOException, CoderException {
		int index = VarInt.decodeInt(inStream);
		Object value = elementCoders.get(index).decode(inStream, context);
		return new RawUnionValue(index, value);
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return null;
	}

	@Override
	public List<? extends Coder<?>> getComponents() {
		return elementCoders;
	}

	/**
	 * Since this coder uses elementCoders.get(index) and coders that are known to run in constant
	 * time, we defer the return value to that coder.
	 */
	@Override
	public boolean isRegisterByteSizeObserverCheap(RawUnionValue union, Context context) {
		int index = getIndexForEncoding(union);
		@SuppressWarnings("unchecked")
		Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
		return coder.isRegisterByteSizeObserverCheap(union.getValue(), context);
	}

	/**
	 * Notifies ElementByteSizeObserver about the byte size of the encoded value using this coder.
	 */
	@Override
	public void registerByteSizeObserver(
			RawUnionValue union, ElementByteSizeObserver observer, Context context)
			throws Exception {
		int index = getIndexForEncoding(union);
		// Write out the union tag.
		observer.update(VarInt.getLength(index));
		// Write out the actual value.
		@SuppressWarnings("unchecked")
		Coder<Object> coder = (Coder<Object>) elementCoders.get(index);
		coder.registerByteSizeObserver(union.getValue(), observer, context);
	}

	/////////////////////////////////////////////////////////////////////////////

	private final List<Coder<?>> elementCoders;

	private UnionCoder(List<Coder<?>> elementCoders) {
		this.elementCoders = elementCoders;
	}

	@Override
	public void verifyDeterministic() throws NonDeterministicException {
		verifyDeterministic(
				"UnionCoder is only deterministic if all element coders are",
				elementCoders);
	}
}
