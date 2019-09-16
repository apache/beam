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
package org.apache.beam.runners.apex.translation;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.util.KryoCloneUtils;
import java.util.List;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.runners.apex.translation.operators.ApexGroupByKeyOperator;
import org.apache.beam.runners.apex.translation.utils.ApexStateInternals;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link ApexGroupByKeyOperator}. */
public class ApexGroupByKeyOperatorTest {

  @Test
  public void testGlobalWindowMinTimestamp() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create().as(ApexPipelineOptions.class);
    options.setRunner(TestApexRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    WindowingStrategy<?, ?> ws =
        WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10)));
    PCollection<KV<String, Integer>> input =
        PCollection.createPrimitiveOutputInternal(
            pipeline, ws, IsBounded.BOUNDED, KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    ApexGroupByKeyOperator<String, Integer> operator =
        new ApexGroupByKeyOperator<>(options, input, new ApexStateInternals.ApexStateBackend());

    operator.setup(null);
    operator.beginWindow(1);
    Assert.assertNotNull("Serialization", operator = KryoCloneUtils.cloneObject(operator));

    final List<Object> results = Lists.newArrayList();
    Sink<Object> sink =
        new Sink<Object>() {
          @Override
          public void put(Object tuple) {
            results.add(tuple);
          }

          @Override
          public int getCount(boolean reset) {
            return 0;
          }
        };
    operator.output.setSink(sink);
    operator.setup(null);
    operator.beginWindow(1);

    Instant windowStart = BoundedWindow.TIMESTAMP_MIN_VALUE;
    BoundedWindow window = new IntervalWindow(windowStart, windowStart.plus(10000));
    PaneInfo paneInfo = PaneInfo.NO_FIRING;

    WindowedValue<KV<String, Integer>> wv1 =
        WindowedValue.of(KV.of("foo", 1), windowStart, window, paneInfo);
    operator.input.process(ApexStreamTuple.DataTuple.of(wv1));

    WindowedValue<KV<String, Integer>> wv2 =
        WindowedValue.of(KV.of("foo", 1), windowStart, window, paneInfo);
    operator.input.process(ApexStreamTuple.DataTuple.of(wv2));

    ApexStreamTuple<WindowedValue<KV<String, Integer>>> watermark =
        ApexStreamTuple.WatermarkTuple.of(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());

    Assert.assertEquals("number outputs", 0, results.size());
    operator.input.process(watermark);
    Assert.assertEquals("number outputs", 2, results.size());
    @SuppressWarnings({"unchecked", "rawtypes"})
    ApexStreamTuple.DataTuple<WindowedValue<KV<String, Iterable<Integer>>>> dataTuple =
        (ApexStreamTuple.DataTuple) results.get(0);
    List<Integer> counts = Lists.newArrayList(1, 1);
    Assert.assertEquals("iterable", KV.of("foo", counts), dataTuple.getValue().getValue());
    Assert.assertEquals("expected watermark", watermark, results.get(1));
  }
}
