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
package org.apache.beam.sdk.extensions.ml;

import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MapStringToDlpRowTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void mapsStringToRow() {
    PCollection<KV<String, Table.Row>> rowCollection =
        testPipeline
            .apply(Create.of(KV.of("key", "value")))
            .apply(ParDo.of(new MapStringToDlpRow(null)));
    PAssert.that(rowCollection)
        .containsInAnyOrder(
            KV.of(
                "key",
                Table.Row.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("value").build())
                    .build()));
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void mapsDelimitedStringToRow() {
    PCollection<KV<String, Table.Row>> rowCollection =
        testPipeline
            .apply(Create.of(KV.of("key", "value,secondValue")))
            .apply(ParDo.of(new MapStringToDlpRow(",")));
    PAssert.that(rowCollection)
        .containsInAnyOrder(
            KV.of(
                "key",
                Table.Row.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("value").build())
                    .addValues(Value.newBuilder().setStringValue("secondValue").build())
                    .build()));
    testPipeline.run().waitUntilFinish();
  }
}
