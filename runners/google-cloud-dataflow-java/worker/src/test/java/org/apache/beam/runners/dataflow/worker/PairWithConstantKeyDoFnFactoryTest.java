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
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PairWithConstantKeyDoFnFactory}. */
@RunWith(JUnit4.class)
public class PairWithConstantKeyDoFnFactoryTest {
  @Test
  public void testConversionOfRecord() throws Exception {
    ParDoFn parDoFn =
        new PairWithConstantKeyDoFnFactory()
            .create(
                null /* pipeline options */,
                CloudObject.fromSpec(
                    ImmutableMap.of(
                        PropertyNames.OBJECT_TYPE_NAME, "PairWithConstantKeyDoFn",
                        WorkerPropertyNames.ENCODED_KEY,
                            StringUtils.byteArrayToJsonString(
                                CoderUtils.encodeToByteArray(BigEndianIntegerCoder.of(), 42)),
                        PropertyNames.ENCODING,
                            ImmutableMap.of(
                                PropertyNames.OBJECT_TYPE_NAME, "kind:fixed_big_endian_int32"))),
                null /* side input infos */,
                null /* main output tag */,
                null /* output tag to receiver index */,
                null /* exection context */,
                null /* operation context */);

    List<Object> outputReceiver = new ArrayList<>();
    parDoFn.startBundle(outputReceiver::add);
    parDoFn.processElement(valueInGlobalWindow(43));
    assertThat(outputReceiver, contains(valueInGlobalWindow(KV.of(42, 43))));
  }
}
