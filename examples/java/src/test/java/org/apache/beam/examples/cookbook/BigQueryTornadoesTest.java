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
package org.apache.beam.examples.cookbook;

import com.google.api.services.bigquery.model.TableRow;
import java.util.List;
import org.apache.beam.examples.cookbook.BigQueryTornadoes.ExtractTornadoesFn;
import org.apache.beam.examples.cookbook.BigQueryTornadoes.FormatCountsFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link BigQueryTornadoes}.
 */
@RunWith(JUnit4.class)
public class BigQueryTornadoesTest {

  @Test
  public void testExtractTornadoes() throws Exception {
    TableRow row = new TableRow()
          .set("month", "6")
          .set("tornado", true);
    DoFnTester<TableRow, Integer> extractWordsFn =
        DoFnTester.of(new ExtractTornadoesFn());
    Assert.assertThat(extractWordsFn.processBundle(row),
                      CoreMatchers.hasItems(6));
  }

  @Test
  public void testNoTornadoes() throws Exception {
    TableRow row = new TableRow()
          .set("month", 6)
          .set("tornado", false);
    DoFnTester<TableRow, Integer> extractWordsFn =
        DoFnTester.of(new ExtractTornadoesFn());
    Assert.assertTrue(extractWordsFn.processBundle(row).isEmpty());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testFormatCounts() throws Exception {
    DoFnTester<KV<Integer, Long>, TableRow> formatCountsFn =
        DoFnTester.of(new FormatCountsFn());
    KV empty[] = {};
    List<TableRow> results = formatCountsFn.processBundle(empty);
    Assert.assertTrue(results.isEmpty());
    KV input[] = { KV.of(3, 0L),
                   KV.of(4, Long.MAX_VALUE),
                   KV.of(5, Long.MIN_VALUE) };
    results = formatCountsFn.processBundle(input);
    Assert.assertEquals(results.size(), 3);
    Assert.assertEquals(results.get(0).get("month"), 3);
    Assert.assertEquals(results.get(0).get("tornado_count"), 0L);
    Assert.assertEquals(results.get(1).get("month"), 4);
    Assert.assertEquals(results.get(1).get("tornado_count"), Long.MAX_VALUE);
    Assert.assertEquals(results.get(2).get("month"), 5);
    Assert.assertEquals(results.get(2).get("tornado_count"), Long.MIN_VALUE);
  }
}
