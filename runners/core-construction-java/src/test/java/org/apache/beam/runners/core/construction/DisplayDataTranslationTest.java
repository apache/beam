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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.runners.core.construction.DisplayDataTranslation}. */
@RunWith(JUnit4.class)
public class DisplayDataTranslationTest {

  @Test
  public void testTranslation() {
    assertThat(
        DisplayDataTranslation.toProto(
            DisplayData.from(
                new HasDisplayData() {

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.add(DisplayData.item("foo", "value"));
                    builder.add(DisplayData.item("foo2", "value2").withLabel("label"));
                  }
                })),
        containsInAnyOrder(
            RunnerApi.DisplayData.newBuilder()
                .setUrn(DisplayDataTranslation.LABELLED_STRING)
                .setPayload(
                    RunnerApi.LabelledStringPayload.newBuilder()
                        .setLabel("foo")
                        .setValue("value")
                        .build()
                        .toByteString())
                .build(),
            RunnerApi.DisplayData.newBuilder()
                .setUrn(DisplayDataTranslation.LABELLED_STRING)
                .setPayload(
                    RunnerApi.LabelledStringPayload.newBuilder()
                        .setLabel("label")
                        .setValue("value2")
                        .build()
                        .toByteString())
                .build()));
  }
}
