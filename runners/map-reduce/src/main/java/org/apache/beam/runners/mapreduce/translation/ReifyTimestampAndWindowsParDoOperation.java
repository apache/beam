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
package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * {@link Operation} that executes {@link ReifyTimestampAndWindowsDoFn}.
 */
public class ReifyTimestampAndWindowsParDoOperation extends ParDoOperation {

  public ReifyTimestampAndWindowsParDoOperation(
      PipelineOptions options,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, new TupleTag<>(), ImmutableList.<TupleTag<?>>of(), windowingStrategy);
  }

  @Override
  DoFn<Object, Object> getDoFn() {
    return (DoFn) new ReifyTimestampAndWindowsDoFn<>();
  }

  public class ReifyTimestampAndWindowsDoFn<K, V>
      extends DoFn<KV<K, V>, KV<K, WindowedValue<V>>> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      KV<K, V> kv = c.element();
      K key = kv.getKey();
      V value = kv.getValue();
      c.output(KV.of(
          key,
          WindowedValue.of(
              value,
              c.timestamp(),
              window,
              c.pane())));
    }
  }
}
