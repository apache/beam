/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link FlatMapFunction} function that filters out those elements that don't belong in this
 * output. We need this to implement MultiOutput ParDo functions in combination with
 * {@link FlinkDoFnFunction}.
 */
public class FlinkMultiOutputPruningFunction<T>
    implements FlatMapFunction<WindowedValue<RawUnionValue>, WindowedValue<T>> {

  private final int ourOutputTag;

  public FlinkMultiOutputPruningFunction(int ourOutputTag) {
    this.ourOutputTag = ourOutputTag;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void flatMap(
      WindowedValue<RawUnionValue> windowedValue,
      Collector<WindowedValue<T>> collector) throws Exception {
    int unionTag = windowedValue.getValue().getUnionTag();
    if (unionTag == ourOutputTag) {
      collector.collect(
          (WindowedValue<T>) windowedValue.withValue(windowedValue.getValue().getValue()));
    }
  }
}
