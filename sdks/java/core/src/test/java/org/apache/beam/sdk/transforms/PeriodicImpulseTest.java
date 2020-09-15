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

import java.util.ArrayList;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesImpulse;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PeriodicImpulse. */
@RunWith(JUnit4.class)
public class PeriodicImpulseTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  public static class ExtractTsDoFn<InputT> extends DoFn<InputT, KV<InputT, Instant>> {
    @ProcessElement
    public void processElement(DoFn<InputT, KV<InputT, Instant>>.ProcessContext c)
        throws Exception {
      c.output(KV.of(c.element(), c.timestamp()));
    }
  }

  @Test
  @Category({
    NeedsRunner.class,
    UsesImpulse.class,
    UsesStatefulParDo.class,
  })
  public void testOutputsProperElements() {
    Instant instant = Instant.now();

    Instant startTime = instant.minus(Duration.standardHours(100));
    long duration = 500;
    Duration interval = Duration.millis(250);
    long intervalMillis = interval.getMillis();
    Instant stopTime = startTime.plus(duration);

    PCollection<KV<Instant, Instant>> result =
        p.apply(PeriodicImpulse.create().startAt(startTime).stopAt(stopTime).withInterval(interval))
            .apply(ParDo.of(new ExtractTsDoFn<>()));

    ArrayList<KV<Instant, Instant>> expectedResults =
        new ArrayList<>((int) (duration / intervalMillis + 1));
    for (long i = 0; i <= duration; i += intervalMillis) {
      Instant el = startTime.plus(i);
      expectedResults.add(KV.of(el, el));
    }

    PAssert.that(result).containsInAnyOrder(expectedResults);

    p.run().waitUntilFinish();
  }
}
