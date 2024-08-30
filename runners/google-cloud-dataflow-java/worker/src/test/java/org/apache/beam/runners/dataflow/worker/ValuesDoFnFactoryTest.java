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
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValuesDoFnFactory}. */
@RunWith(JUnit4.class)
public class ValuesDoFnFactoryTest {

  @Test
  public void testConversionOfRecord() throws Exception {
    ParDoFn parDoFn =
        new ValuesDoFnFactory()
            .create(
                null /* pipeline options */,
                CloudObject.fromSpec(ImmutableMap.of(PropertyNames.OBJECT_TYPE_NAME, "ValuesDoFn")),
                null /* side input infos */,
                null /* main output tag */,
                null /* output tag to receiver index */,
                null /* exection context */,
                null /* operation context */);

    List<Object> outputReceiver = new ArrayList<>();
    parDoFn.startBundle(outputReceiver::add);
    parDoFn.processElement(valueInGlobalWindow(KV.of(42, 43)));
    assertThat(outputReceiver, contains(valueInGlobalWindow(43)));
  }
}
