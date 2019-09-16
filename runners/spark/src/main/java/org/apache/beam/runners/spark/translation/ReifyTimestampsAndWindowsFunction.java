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
package org.apache.beam.runners.spark.translation;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.Function;

/**
 * Simple {@link Function} to bring the windowing information into the value from the implicit
 * background representation of the {@link PCollection}.
 */
public class ReifyTimestampsAndWindowsFunction<K, V>
    implements Function<WindowedValue<KV<K, V>>, KV<K, WindowedValue<V>>> {
  @Override
  public KV<K, WindowedValue<V>> call(WindowedValue<KV<K, V>> elem) throws Exception {
    return KV.of(
        elem.getValue().getKey(),
        WindowedValue.of(
            elem.getValue().getValue(), elem.getTimestamp(), elem.getWindows(), elem.getPane()));
  }
}
