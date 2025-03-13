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
package org.apache.beam.sdk.schemas.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeadLetteredTransformTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String FAILURE_KEY = "KLJSDHFLKJDHF";

  private static final List<Failure> FAILURES = new ArrayList<>();

  private static synchronized void capture(Failure val) {
    FAILURES.add(val);
  }

  private static synchronized List<Failure> getFailures() {
    return ImmutableList.copyOf(FAILURES);
  }

  private static synchronized void resetFailures() {
    FAILURES.clear();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDeadLettersOnlyFailures() throws Exception {
    resetFailures();
    PCollection<Long> elements = p.apply(Create.of(10L, 20L).withCoder(VarLongCoder.of()));
    PCollection<Long> results =
        elements.apply(
            new DeadLetteredTransform<>(
                SimpleFunction.fromSerializableFunctionWithOutputType(
                    x -> {
                      if (x == 10L) {
                        throw new RuntimeException(FAILURE_KEY);
                      }
                      return x;
                    },
                    TypeDescriptor.of(Long.class)),
                new PTransform<PCollection<Failure>, PDone>() {
                  @Override
                  public PDone expand(PCollection<Failure> input) {
                    input.apply(
                        MapElements.into(TypeDescriptor.of(Void.class))
                            .via(
                                failure -> {
                                  capture(failure);
                                  return null;
                                }));
                    return PDone.in(input.getPipeline());
                  }
                }));
    PAssert.that(results).containsInAnyOrder(20L);
    p.run().waitUntilFinish();
    List<Failure> failures = getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.iterator().next();
    assertEquals(
        10L, VarLongCoder.of().decode(new ByteArrayInputStream(failure.getPayload())).longValue());
    assertTrue(failure.getError().contains(FAILURE_KEY));
  }
}
