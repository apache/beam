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
package com.dataartisans.flink.dataflow.translation.wrappers.streaming;

import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.util.*;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

public class FlinkGroupByKeyWrapper {

	/**
	 * Just an auxiliary interface to bypass the fact that java anonymous classes cannot implement
	 * multiple interfaces.
	 */
	private interface KeySelectorWithQueryableResultType<K, V> extends KeySelector<WindowedValue<KV<K, V>>, K>, ResultTypeQueryable<K> {
	}

	public static <K, V> KeyedStream<WindowedValue<KV<K, V>>, K> groupStreamByKey(DataStream<WindowedValue<KV<K, V>>> inputDataStream, KvCoder<K, V> inputKvCoder) {
		final Coder<K> keyCoder = inputKvCoder.getKeyCoder();
		final TypeInformation<K> keyTypeInfo = new CoderTypeInformation<>(keyCoder);

		return inputDataStream.keyBy(
				new KeySelectorWithQueryableResultType<K, V>() {

					@Override
					public K getKey(WindowedValue<KV<K, V>> value) throws Exception {
						return value.getValue().getKey();
					}

					@Override
					public TypeInformation<K> getProducedType() {
						return keyTypeInfo;
					}
				});
	}
}
