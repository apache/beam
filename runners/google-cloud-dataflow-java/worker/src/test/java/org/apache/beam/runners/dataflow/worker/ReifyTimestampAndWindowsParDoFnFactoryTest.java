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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link ReifyTimestampAndWindowsParDoFnFactory} */
public class ReifyTimestampAndWindowsParDoFnFactoryTest {

  private <K, V> void verifyReifiedIsInTheSameWindows(WindowedValue<KV<K, V>> elem)
      throws Exception {
    ParDoFn reifyFn =
        new ReifyTimestampAndWindowsParDoFnFactory()
            .create(null, null, null, null, null, null, null);

    SingleValueReceiver<WindowedValue<KV<K, WindowedValue<V>>>> receiver =
        new SingleValueReceiver<>();

    reifyFn.startBundle(receiver);

    // The important thing to test that is not just a restatement of the ParDoFn is that
    // it only produces one element per input
    reifyFn.processElement(elem);

    assertThat(receiver.reified.getValue().getKey(), equalTo(elem.getValue().getKey()));
    assertThat(
        receiver.reified.getValue().getValue().getValue(), equalTo(elem.getValue().getValue()));
    assertThat(receiver.reified.getValue().getValue().getTimestamp(), equalTo(elem.getTimestamp()));
    assertThat(receiver.reified.getValue().getValue().getWindows(), equalTo(elem.getWindows()));
    assertThat(receiver.reified.getValue().getValue().getPane(), equalTo(elem.getPane()));
  }

  @Test
  public void testSingleWindow() throws Exception {
    verifyReifiedIsInTheSameWindows(
        WindowedValue.of(
            KV.of(42, "bizzle"),
            new Instant(73),
            new IntervalWindow(new Instant(5), new Instant(15)),
            PaneInfo.NO_FIRING));
  }

  @Test
  public void testMultiWindowStaysCompressed() throws Exception {
    verifyReifiedIsInTheSameWindows(
        WindowedValue.of(
            KV.of(42, "bizzle"),
            new Instant(73),
            ImmutableList.of(
                new IntervalWindow(new Instant(5), new Instant(15)),
                new IntervalWindow(new Instant(17), new Instant(97))),
            PaneInfo.NO_FIRING));
  }

  private static class SingleValueReceiver<T> implements Receiver {

    public @Nullable T reified = null;

    @Override
    public void process(Object outputElem) throws Exception {
      checkState(reified == null, "Element output more than once");
      reified = (T) outputElem;
    }
  }
}
