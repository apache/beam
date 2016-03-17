/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Java 8 tests for {@link WithTimestamps}.
 */
@RunWith(JUnit4.class)
public class WithTimestampsJava8Test implements Serializable {
  @Test
  @Category(RunnableOnService.class)
  public void withTimestampsLambdaShouldApplyTimestamps() {
    TestPipeline p = TestPipeline.create();

    String yearTwoThousand = "946684800000";
    PCollection<String> timestamped =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
         .apply(WithTimestamps.of((String input) -> new Instant(Long.valueOf(yearTwoThousand))));

    PCollection<KV<String, Instant>> timestampedVals =
        timestamped.apply(ParDo.of(new DoFn<String, KV<String, Instant>>() {
          @Override
          public void processElement(DoFn<String, KV<String, Instant>>.ProcessContext c)
              throws Exception {
            c.output(KV.of(c.element(), c.timestamp()));
          }
        }));

    DataflowAssert.that(timestamped)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    DataflowAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0)),
            KV.of("1234", new Instant("1234")),
            KV.of(Integer.toString(Integer.MAX_VALUE), new Instant(Integer.MAX_VALUE)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand))));
  }
}

