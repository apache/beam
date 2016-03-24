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

package com.google.cloud.dataflow.examples.complete;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/** Unit tests for {@link TopWikipediaSessions}. */
@RunWith(JUnit4.class)
public class TopWikipediaSessionsTest {
  @Test
  @Category(RunnableOnService.class)
  public void testComputeTopUsers() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        p.apply(Create.of(Arrays.asList(
            new TableRow().set("timestamp", 0).set("contributor_username", "user1"),
            new TableRow().set("timestamp", 1).set("contributor_username", "user1"),
            new TableRow().set("timestamp", 2).set("contributor_username", "user1"),
            new TableRow().set("timestamp", 0).set("contributor_username", "user2"),
            new TableRow().set("timestamp", 1).set("contributor_username", "user2"),
            new TableRow().set("timestamp", 3601).set("contributor_username", "user2"),
            new TableRow().set("timestamp", 3602).set("contributor_username", "user2"),
            new TableRow().set("timestamp", 35 * 24 * 3600).set("contributor_username", "user3"))))
        .apply(new TopWikipediaSessions.ComputeTopSessions(1.0));

    DataflowAssert.that(output).containsInAnyOrder(Arrays.asList(
        "user1 : [1970-01-01T00:00:00.000Z..1970-01-01T01:00:02.000Z)"
        + " : 3 : 1970-01-01T00:00:00.000Z",
        "user3 : [1970-02-05T00:00:00.000Z..1970-02-05T01:00:00.000Z)"
        + " : 1 : 1970-02-01T00:00:00.000Z"));

    p.run();
  }
}
