/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * A FlatMap function that filters out those elements that don't belong in this output. We need
 * this to implement MultiOutput ParDo functions.
 */
public class MultiOutputPruningFunction<T> implements FlatMapFunction<RawUnionValue, T> {

	private final int outputTag;

	public MultiOutputPruningFunction(int outputTag) {
		this.outputTag = outputTag;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void flatMap(RawUnionValue rawUnionValue, Collector<T> collector) throws Exception {
		if (rawUnionValue.getUnionTag() == outputTag) {
			collector.collect((T) rawUnionValue.getValue());
		}
	}
}
