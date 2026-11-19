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
package org.apache.beam.examples;

import java.math.BigDecimal;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link VariantEventsExample}. */
@RunWith(JUnit4.class)
public class VariantEventsExampleTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testRevenuePerUser() {
    PCollection<KV<String, BigDecimal>> revenue =
        pipeline
            .apply(Create.of(VariantEventsExample.SAMPLE_EVENTS))
            .apply(new VariantEventsExample.RevenuePerUser());

    // ali: 19.99 + 5.01, amy: 7.50. John's purchase has a non-numeric amount and is skipped.
    PAssert.that(revenue)
        .containsInAnyOrder(
            KV.of("ali", new BigDecimal("25.00")), KV.of("amy", new BigDecimal("7.50")));
    pipeline.run().waitUntilFinish();
  }
}
