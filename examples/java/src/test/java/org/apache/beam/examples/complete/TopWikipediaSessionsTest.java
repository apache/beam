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
package org.apache.beam.examples.complete;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TopWikipediaSessions}. */
@RunWith(JUnit4.class)
public class TopWikipediaSessionsTest {

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testComputeTopUsers() {

    PCollection<String> output =
        p.apply(
                Create.of(
                    Arrays.asList(
                        new TableRow().set("timestamp", 0).set("contributor_username", "user1"),
                        new TableRow().set("timestamp", 1).set("contributor_username", "user1"),
                        new TableRow().set("timestamp", 2).set("contributor_username", "user1"),
                        new TableRow().set("timestamp", 0).set("contributor_username", "user2"),
                        new TableRow().set("timestamp", 1).set("contributor_username", "user2"),
                        new TableRow().set("timestamp", 3601).set("contributor_username", "user2"),
                        new TableRow().set("timestamp", 3602).set("contributor_username", "user2"),
                        new TableRow()
                            .set("timestamp", 35 * 24 * 3600)
                            .set("contributor_username", "user3"))))
            .apply(new TopWikipediaSessions.ComputeTopSessions(1.0));

    PAssert.that(output)
        .containsInAnyOrder(
            Arrays.asList(
                "user1 : [1970-01-01T00:00:00.000Z..1970-01-01T01:00:02.000Z)"
                    + " : 3 : 1970-01-01T00:00:00.000Z",
                "user3 : [1970-02-05T00:00:00.000Z..1970-02-05T01:00:00.000Z)"
                    + " : 1 : 1970-02-01T00:00:00.000Z"));

    p.run().waitUntilFinish();
  }
}
