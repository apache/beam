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

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test {@link BoundedEventSource}. */
@RunWith(JUnit4.class)
public class BoundedEventSourceTest {
  private GeneratorConfig makeConfig(long n) {
    return new GeneratorConfig(NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, n, 0);
  }

  @Test
  public void sourceAndReadersWork() throws Exception {
    NexmarkOptions options = PipelineOptionsFactory.as(NexmarkOptions.class);
    long n = 200L;
    BoundedEventSource source = new BoundedEventSource(makeConfig(n), 1);

    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(
        source.createReader(options), options);
  }

  @Test
  public void splitAtFractionRespectsContract() throws Exception {
    NexmarkOptions options = PipelineOptionsFactory.as(NexmarkOptions.class);
    long n = 20L;
    BoundedEventSource source = new BoundedEventSource(makeConfig(n), 1);

    // Can't split if already consumed.
    SourceTestUtils.assertSplitAtFractionFails(source, 10, 0.3, options);

    SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(source, 5, 0.3, options);

    SourceTestUtils.assertSplitAtFractionExhaustive(source, options);
  }

  @Test
  public void splitIntoBundlesRespectsContract() throws Exception {
    NexmarkOptions options = PipelineOptionsFactory.as(NexmarkOptions.class);
    long n = 200L;
    BoundedEventSource source = new BoundedEventSource(makeConfig(n), 1);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(10, options), options);
  }
}
