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
package org.apache.beam.fn.harness.jmh;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.fn.harness.jmh.CachesBenchmark.CursorState;
import org.apache.beam.fn.harness.jmh.CachesBenchmark.HarnessCacheState;
import org.apache.beam.fn.harness.jmh.CachesBenchmark.SharedCacheState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Smoke tests for {@link CachesBenchmark}. */
@RunWith(JUnit4.class)
public class CachesBenchmarkTest {

  @Test
  public void testAllBenchmarksExecute() throws Exception {
    CachesBenchmark benchmark = new CachesBenchmark();
    HarnessCacheState state = new HarnessCacheState();
    state.setUp();
    assertTrue(benchmark.weighString(state) > 0);
    assertTrue(benchmark.weighLargeString(state) >= 16 * 1024);
    assertTrue(benchmark.weighByteString(state) > 0);
    assertTrue(benchmark.weighByteArray(state) > 0);
    assertTrue(benchmark.weighLong(state) > 0);
    for (int i = 0; i < 16; i++) {
      assertTrue(benchmark.weighMixed(state) > 0);
    }

    assertNotNull(benchmark.subCacheLookupMultiKey(state));
    assertNotNull(benchmark.subCacheCreateAndLookup(state));
    assertNotNull(benchmark.clearableNestedSubCacheLifecycle(state));
    assertTrue(benchmark.clearableEvictionChurn(state) > 0);

    SharedCacheState shared = new SharedCacheState();
    shared.setUp();
    CursorState cursor = new CursorState();
    cursor.setUp();
    assertNotNull(benchmark.sharedSubCacheLookupContended(shared, cursor));
    assertNotNull(benchmark.sharedClearablePutClearContended(shared, cursor));
  }
}
