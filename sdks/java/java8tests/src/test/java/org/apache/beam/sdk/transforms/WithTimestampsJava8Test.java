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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 tests for {@link WithTimestamps}.
 */
@RunWith(JUnit4.class)
public class WithTimestampsJava8Test implements Serializable {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void withTimestampsLambdaShouldApplyTimestamps() {

    final String yearTwoThousand = "946684800000";
    PCollection<String> timestamped =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
         .apply(WithTimestamps.of((String input) -> new Instant(Long.valueOf(input))));

    PCollection<KV<String, Instant>> timestampedVals =
        timestamped.apply(ParDo.of(new DoFn<String, KV<String, Instant>>() {
          @ProcessElement
          public void processElement(ProcessContext c)
              throws Exception {
            c.output(KV.of(c.element(), c.timestamp()));
          }
        }));

    PAssert.that(timestamped)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    PAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0)),
            KV.of("1234", new Instant(Long.valueOf("1234"))),
            KV.of(Integer.toString(Integer.MAX_VALUE), new Instant(Integer.MAX_VALUE)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand))));

    p.run();
  }
}
