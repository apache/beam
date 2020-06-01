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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Explode {@link WindowedValue} that belongs to multiple windows into multiple "single window"
 * {@link WindowedValue values}, so we can safely group elements by (K, W) tuples.
 *
 * @param <T> Value type.
 */
public class FlinkExplodeWindowsFunction<T>
    implements FlatMapFunction<WindowedValue<T>, WindowedValue<T>> {

  @Override
  public void flatMap(WindowedValue<T> value, Collector<WindowedValue<T>> coll) {
    value.explodeWindows().forEach(coll::collect);
  }
}
