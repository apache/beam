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

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.util.List;
import org.apache.beam.examples.cookbook.CombinePerKeyExamples.ExtractLargeWordsFn;
import org.apache.beam.examples.cookbook.CombinePerKeyExamples.FormatShakespeareOutputFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CombinePerKeyExamples}. */
@RunWith(JUnit4.class)
public class CombinePerKeyExamplesTest {

  private static final TableRow row1 =
      new TableRow().set("corpus", "king_lear").set("word", "snuffleupaguses");
  private static final TableRow row2 =
      new TableRow().set("corpus", "macbeth").set("word", "antidisestablishmentarianism");
  private static final TableRow row3 =
      new TableRow().set("corpus", "king_lear").set("word", "antidisestablishmentarianism");
  private static final TableRow row4 = new TableRow().set("corpus", "macbeth").set("word", "bob");
  private static final TableRow row5 = new TableRow().set("corpus", "king_lear").set("word", "hi");

  static final TableRow[] ROWS_ARRAY = new TableRow[] {row1, row2, row3, row4, row5};

  private static final KV<String, String> tuple1 = KV.of("snuffleupaguses", "king_lear");
  private static final KV<String, String> tuple2 = KV.of("antidisestablishmentarianism", "macbeth");
  private static final KV<String, String> tuple3 =
      KV.of("antidisestablishmentarianism", "king_lear");

  private static final KV<String, String> combinedTuple1 =
      KV.of("antidisestablishmentarianism", "king_lear,macbeth");
  private static final KV<String, String> combinedTuple2 = KV.of("snuffleupaguses", "king_lear");

  @SuppressWarnings({"unchecked", "rawtypes"})
  static final KV<String, String>[] COMBINED_TUPLES_ARRAY =
      new KV[] {combinedTuple1, combinedTuple2};

  private static final TableRow resultRow1 =
      new TableRow().set("word", "snuffleupaguses").set("all_plays", "king_lear");
  private static final TableRow resultRow2 =
      new TableRow()
          .set("word", "antidisestablishmentarianism")
          .set("all_plays", "king_lear,macbeth");

  @Test
  public void testExtractLargeWordsFn() throws Exception {
    DoFnTester<TableRow, KV<String, String>> extractLargeWordsFn =
        DoFnTester.of(new ExtractLargeWordsFn());
    List<KV<String, String>> results = extractLargeWordsFn.processBundle(ROWS_ARRAY);
    assertThat(results, CoreMatchers.hasItem(tuple1));
    assertThat(results, CoreMatchers.hasItem(tuple2));
    assertThat(results, CoreMatchers.hasItem(tuple3));
  }

  @Test
  public void testFormatShakespeareOutputFn() throws Exception {
    DoFnTester<KV<String, String>, TableRow> formatShakespeareOutputFn =
        DoFnTester.of(new FormatShakespeareOutputFn());
    List<TableRow> results = formatShakespeareOutputFn.processBundle(COMBINED_TUPLES_ARRAY);
    assertThat(results, CoreMatchers.hasItem(resultRow1));
    assertThat(results, CoreMatchers.hasItem(resultRow2));
  }
}
