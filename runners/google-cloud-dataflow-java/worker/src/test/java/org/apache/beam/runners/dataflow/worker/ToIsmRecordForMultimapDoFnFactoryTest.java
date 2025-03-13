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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ToIsmRecordForMultimapDoFnFactory}. */
@RunWith(JUnit4.class)
public class ToIsmRecordForMultimapDoFnFactoryTest {
  @Test
  public void testConversionOfRecord() throws Exception {
    ParDoFn parDoFn =
        new ToIsmRecordForMultimapDoFnFactory()
            .create(
                null /* pipeline options */,
                CloudObject.fromSpec(
                    ImmutableMap.of(
                        PropertyNames.OBJECT_TYPE_NAME,
                        "ToIsmRecordForMultimapDoFn",
                        PropertyNames.ENCODING,
                        createIsmRecordEncoding())),
                null /* side input infos */,
                null /* main output tag */,
                null /* output tag to receiver index */,
                null /* exection context */,
                null /* operation context */);

    List<Object> outputReceiver = new ArrayList<>();
    parDoFn.startBundle(outputReceiver::add);
    parDoFn.processElement(
        valueInGlobalWindow(
            KV.of(
                12 /* shard key */,
                ImmutableList.of(
                    KV.of(
                        KV.of(42 /* user key */, GlobalWindow.INSTANCE /* window */) /* sort key */,
                        4 /* value */),
                    KV.of(
                        KV.of(42 /* user key */, GlobalWindow.INSTANCE /* window */) /* sort key */,
                        5 /* value */),
                    KV.of(
                        KV.of(43 /* user key */, GlobalWindow.INSTANCE /* window */) /* sort key */,
                        6 /* value */),
                    KV.of(
                        KV.of(44 /* user key */, GlobalWindow.INSTANCE /* window */) /* sort key */,
                        7 /* value */),
                    KV.of(
                        KV.of(44 /* user key */, GlobalWindow.INSTANCE /* window */) /* sort key */,
                        8 /* value */)))));
    assertThat(
        outputReceiver,
        contains(
            valueInGlobalWindow(IsmRecord.of(ImmutableList.of(42, GlobalWindow.INSTANCE, 0L), 4)),
            /* same structural value  as above */
            valueInGlobalWindow(IsmRecord.of(ImmutableList.of(42, GlobalWindow.INSTANCE, 1L), 5)),
            valueInGlobalWindow(IsmRecord.of(ImmutableList.of(43, GlobalWindow.INSTANCE, 0L), 6)),
            valueInGlobalWindow(IsmRecord.of(ImmutableList.of(44, GlobalWindow.INSTANCE, 0L), 7)),
            /* same structural value as above and final value */
            valueInGlobalWindow(IsmRecord.of(ImmutableList.of(44, GlobalWindow.INSTANCE, 1L), 8))));
  }

  private Map<String, Object> createIsmRecordEncoding() {
    return ImmutableMap.of(
        PropertyNames.OBJECT_TYPE_NAME,
        "kind:ism_record",
        "num_shard_key_coders",
        1L,
        PropertyNames.COMPONENT_ENCODINGS,
        ImmutableList.of(
            ImmutableMap.of("@type", "kind:var_int32"),
            ImmutableMap.of("@type", "kind:global_window"),
            ImmutableMap.of("@type", "kind:fixed_big_endian_int32"),
            ImmutableMap.of("@type", "kind:fixed_big_endian_int64")));
  }
}
