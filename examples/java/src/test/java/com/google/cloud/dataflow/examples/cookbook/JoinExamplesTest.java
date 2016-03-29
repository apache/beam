/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.cookbook;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.cookbook.JoinExamples.ExtractCountryInfoFn;
import com.google.cloud.dataflow.examples.cookbook.JoinExamples.ExtractEventDataFn;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link JoinExamples}. */
@RunWith(JUnit4.class)
public class JoinExamplesTest {

  private static final TableRow row1 = new TableRow()
        .set("ActionGeo_CountryCode", "VM").set("SQLDATE", "20141212")
        .set("Actor1Name", "BANGKOK").set("SOURCEURL", "http://cnn.com");
  private static final TableRow row2 = new TableRow()
        .set("ActionGeo_CountryCode", "VM").set("SQLDATE", "20141212")
        .set("Actor1Name", "LAOS").set("SOURCEURL", "http://www.chicagotribune.com");
  private static final TableRow row3 = new TableRow()
        .set("ActionGeo_CountryCode", "BE").set("SQLDATE", "20141213")
        .set("Actor1Name", "AFGHANISTAN").set("SOURCEURL", "http://cnn.com");
  static final TableRow[] EVENTS = new TableRow[] {
    row1, row2, row3
  };
  static final List<TableRow> EVENT_ARRAY = Arrays.asList(EVENTS);

  private static final KV<String, String> kv1 = KV.of("VM",
      "Date: 20141212, Actor1: LAOS, url: http://www.chicagotribune.com");
  private static final KV<String, String> kv2 = KV.of("BE",
      "Date: 20141213, Actor1: AFGHANISTAN, url: http://cnn.com");
  private static final KV<String, String> kv3 = KV.of("BE", "Belgium");
  private static final KV<String, String> kv4 = KV.of("VM", "Vietnam");

  private static final TableRow cc1 = new TableRow()
        .set("FIPSCC", "VM").set("HumanName", "Vietnam");
  private static final TableRow cc2 = new TableRow()
        .set("FIPSCC", "BE").set("HumanName", "Belgium");
  static final TableRow[] CCS = new TableRow[] {
    cc1, cc2
  };
  static final List<TableRow> CC_ARRAY = Arrays.asList(CCS);

  static final String[] JOINED_EVENTS = new String[] {
      "Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: LAOS, "
          + "url: http://www.chicagotribune.com",
      "Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: BANGKOK, "
          + "url: http://cnn.com",
      "Country code: BE, Country name: Belgium, Event info: Date: 20141213, Actor1: AFGHANISTAN, "
          + "url: http://cnn.com"
    };

  @Test
  public void testExtractEventDataFn() {
    DoFnTester<TableRow, KV<String, String>> extractEventDataFn =
        DoFnTester.of(new ExtractEventDataFn());
    List<KV<String, String>> results = extractEventDataFn.processBatch(EVENTS);
    Assert.assertThat(results, CoreMatchers.hasItem(kv1));
    Assert.assertThat(results, CoreMatchers.hasItem(kv2));
  }

  @Test
  public void testExtractCountryInfoFn() {
    DoFnTester<TableRow, KV<String, String>> extractCountryInfoFn =
        DoFnTester.of(new ExtractCountryInfoFn());
    List<KV<String, String>> results = extractCountryInfoFn.processBatch(CCS);
    Assert.assertThat(results, CoreMatchers.hasItem(kv3));
    Assert.assertThat(results, CoreMatchers.hasItem(kv4));
  }


  @Test
  @Category(RunnableOnService.class)
  public void testJoin() throws java.lang.Exception {
    Pipeline p = TestPipeline.create();
    PCollection<TableRow> input1 = p.apply("CreateEvent", Create.of(EVENT_ARRAY));
    PCollection<TableRow> input2 = p.apply("CreateCC", Create.of(CC_ARRAY));

    PCollection<String> output = JoinExamples.joinEvents(input1, input2);
    DataflowAssert.that(output).containsInAnyOrder(JOINED_EVENTS);
    p.run();
  }
}
