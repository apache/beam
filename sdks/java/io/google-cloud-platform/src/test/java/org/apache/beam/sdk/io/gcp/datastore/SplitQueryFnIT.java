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

package org.apache.beam.sdk.io.gcp.datastore;

import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.NUM_QUERY_SPLITS_MIN;
import static org.junit.Assert.assertEquals;

import com.google.datastore.v1.Query;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.SplitQueryFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.V1Options;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link DatastoreV1.Read.SplitQueryFn}.
 *
 * Note: It is hard to mock the exact behavior of Datastore, especially for the statistics queries.
 * Also the fact that DatastoreIO falls back gracefully when querying statistics fails, makes it
 * hard to catch these issues in production. This test here ensures we interact with the Datastore
 * directly, query the actual stats and verify that the SplitQueryFn generates the expected number
 * of query splits.
 */
@RunWith(JUnit4.class)
public class SplitQueryFnIT {
  /**
   * Tests {@link SplitQueryFn} to generate expected number of splits for a large dataset.
   */
  @Test
  public void testSplitQueryFnWithLargeDataset() throws Exception {
    String projectId = "apache-beam-testing";
    String kind = "input_sort_1G";
    String namespace = null;
    // Num splits is computed based on the entity_bytes size of the input_sort_1G kind reported by
    // Datastore stats.
    int exceptedNumSplits = 19;
    testSplitQueryFn(projectId, kind, namespace, exceptedNumSplits);
  }

  /**
   * Tests {@link SplitQueryFn} to fallback to NUM_QUERY_SPLITS_MIN for a small dataset.
   */
  @Test
  public void testSplitQueryFnWithSmallDataset() throws Exception {
    String projectId = "deft-testing-integration2";
    String kind = "shakespeare-demo";
    String namespace = null;
    int exceptedNumSplits = NUM_QUERY_SPLITS_MIN;
    testSplitQueryFn(projectId, kind, namespace, exceptedNumSplits);
  }

  /**
   * A helper method to test {@link SplitQueryFn} to generate the expected number of splits.
   */
  private void testSplitQueryFn(String projectId, String kind, @Nullable String namespace,
      int expectedNumSplits) throws Exception {
    Query.Builder query = Query.newBuilder();
    query.addKindBuilder().setName(kind);

    SplitQueryFn splitQueryFn = new SplitQueryFn(
        V1Options.from(projectId, query.build(), namespace), 0);
    DoFnTester<Query, KV<Integer, Query>> doFnTester = DoFnTester.of(splitQueryFn);

    List<KV<Integer, Query>> queries = doFnTester.processBundle(query.build());
    assertEquals(queries.size(), expectedNumSplits);
  }

  // TODO (vikasrk): Create datasets under a different namespace and add tests.
}
