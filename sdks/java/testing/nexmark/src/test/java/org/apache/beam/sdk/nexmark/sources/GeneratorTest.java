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
package org.apache.beam.sdk.nexmark.sources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test {@link Generator}. */
@RunWith(JUnit4.class)
public class GeneratorTest {
  private GeneratorConfig makeConfig(long n) {
    return new GeneratorConfig(NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
  }

  private <T> long consume(long n, Iterator<T> itr) {
    for (long i = 0; i < n; i++) {
      assertTrue(itr.hasNext());
      itr.next();
    }
    return n;
  }

  private <T> long consume(Iterator<T> itr) {
    long n = 0;
    while (itr.hasNext()) {
      itr.next();
      n++;
    }
    return n;
  }

  @Test
  public void splitAtFractionPreservesOverallEventCount() {
    long n = 55729L;
    GeneratorConfig initialConfig = makeConfig(n);
    long expected = initialConfig.getStopEventId() - initialConfig.getStartEventId();

    long actual = 0;

    Generator initialGenerator = new Generator(initialConfig);

    // Consume some events.
    actual += consume(5000, initialGenerator);

    // Split once.
    GeneratorConfig remainConfig1 = initialGenerator.splitAtEventId(9000L);
    Generator remainGenerator1 = new Generator(remainConfig1);

    // Consume some more events.
    actual += consume(2000, initialGenerator);
    actual += consume(3000, remainGenerator1);

    // Split again.
    GeneratorConfig remainConfig2 = remainGenerator1.splitAtEventId(30000L);
    Generator remainGenerator2 = new Generator(remainConfig2);

    // Run to completion.
    actual += consume(initialGenerator);
    actual += consume(remainGenerator1);
    actual += consume(remainGenerator2);

    assertEquals(expected, actual);
  }

  @Test
  public void splitPreservesOverallEventCount() {
    long n = 51237L;
    GeneratorConfig initialConfig = makeConfig(n);
    long expected = initialConfig.getStopEventId() - initialConfig.getStartEventId();

    List<Generator> generators = new ArrayList<>();
    for (GeneratorConfig subConfig : initialConfig.split(20)) {
      generators.add(new Generator(subConfig));
    }

    long actual = 0;
    for (Generator generator : generators) {
      actual += consume(generator);
    }

    assertEquals(expected, actual);
  }
}
