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
package org.apache.beam.runners.samza.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Test;

/** Unit tests for {@link WindowUtils}. */
public class WindowUtilsTest {

  @Test
  public void testGetWindowStrategy() throws IOException {
    SdkComponents components = SdkComponents.create();
    String environmentId =
        components.registerEnvironment(Environments.createDockerEnvironment("java"));
    WindowingStrategy<Object, IntervalWindow> expected =
        WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1)))
            .withMode(WindowingStrategy.AccumulationMode.DISCARDING_FIRED_PANES)
            .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
            .withAllowedLateness(Duration.ZERO)
            .withEnvironmentId(environmentId);
    components.registerWindowingStrategy(expected);
    String collectionId =
        components.registerPCollection(
            PCollection.createPrimitiveOutputInternal(
                    Pipeline.create(), expected, PCollection.IsBounded.BOUNDED, VoidCoder.of())
                .setName("name"));

    WindowingStrategy<?, ?> actual =
        WindowUtils.getWindowStrategy(collectionId, components.toComponents());

    assertEquals(expected, actual);
  }

  @Test
  public void testInstantiateWindowedCoder() throws IOException {
    Coder<KV<Long, String>> expectedValueCoder =
        KvCoder.of(VarLongCoder.of(), StringUtf8Coder.of());
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    String collectionId =
        components.registerPCollection(
            PCollection.createPrimitiveOutputInternal(
                    Pipeline.create(),
                    WindowingStrategy.globalDefault(),
                    PCollection.IsBounded.BOUNDED,
                    expectedValueCoder)
                .setName("name"));

    assertEquals(
        expectedValueCoder,
        WindowUtils.instantiateWindowedCoder(collectionId, components.toComponents())
            .getValueCoder());
  }
}
