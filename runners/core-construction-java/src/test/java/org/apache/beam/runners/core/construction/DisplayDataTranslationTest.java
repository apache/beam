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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for utilities in {@link DisplayDataTranslation}. */
@RunWith(Parameterized.class)
public class DisplayDataTranslationTest {

  @Parameters(name = "{index}: {0}")
  public static Iterable<DisplayData.Item> items() {
    return ImmutableList.of(
        DisplayData.Item.raw(
            DisplayData.Path.absolute("bizzle"), "key", "value", "", "", ""));
  }

  @Parameter(0)
  public DisplayData.Item itemUnderTest;

  @Test
  public void testToProtoAndBack() throws Exception {
    DisplayData.Item toProtoAndBackItem =
        DisplayDataTranslation.fromProto(DisplayDataTranslation.toProto(itemUnderTest));
    assertThat(toProtoAndBackItem, equalTo(itemUnderTest));
  }
}