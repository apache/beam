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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * This flat map function bootstraps from collection elements and turns them into WindowedValues
 * (as required by the Flink runner).
 */
public class FlinkStreamingCreateFunction<IN, OUT> implements FlatMapFunction<IN, WindowedValue<OUT>> {

  private final List<byte[]> elements;
  private final Coder<OUT> coder;

  public FlinkStreamingCreateFunction(List<byte[]> elements, Coder<OUT> coder) {
    this.elements = elements;
    this.coder = coder;
  }

  @Override
  public void flatMap(IN value, Collector<WindowedValue<OUT>> out) throws Exception {

    for (byte[] element : elements) {
      ByteArrayInputStream bai = new ByteArrayInputStream(element);
      OUT outValue = coder.decode(bai, Coder.Context.OUTER);

      out.collect(WindowedValue.of(outValue, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
    }
  }
}
