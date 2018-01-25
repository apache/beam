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
package org.apache.beam.examples;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Snippets.
 */
@RunWith(JUnit4.class)
public class SnippetsTest implements Serializable {

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  /* Tests CoGroupByKeyTuple */
  @Test
  public void testCoGroupByKeyTuple() throws IOException {
    // [START CoGroupByKeyTupleInputs]
    final List<KV<String, String>> emailsList = Arrays.asList(
        KV.of("amy", "amy@example.com"),
        KV.of("carl", "carl@example.com"),
        KV.of("julia", "julia@example.com"),
        KV.of("carl", "carl@email.com"));

    final List<KV<String, String>> phonesList = Arrays.asList(
        KV.of("amy", "111-222-3333"),
        KV.of("james", "222-333-4444"),
        KV.of("amy", "333-444-5555"),
        KV.of("carl", "444-555-6666"));

    PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
    PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));
    // [END CoGroupByKeyTupleInputs]

    // [START CoGroupByKeyTupleOutputs]
    final TupleTag<String> emailsTag = new TupleTag();
    final TupleTag<String> phonesTag = new TupleTag();

    final List<KV<String, CoGbkResult>> expectedResults = Arrays.asList(
        KV.of("amy", CoGbkResult
          .of(emailsTag, Arrays.asList("amy@example.com"))
          .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))),
        KV.of("carl", CoGbkResult
          .of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
          .and(phonesTag, Arrays.asList("444-555-6666"))),
        KV.of("james", CoGbkResult
          .of(emailsTag, Arrays.asList())
          .and(phonesTag, Arrays.asList("222-333-4444"))),
        KV.of("julia", CoGbkResult
          .of(emailsTag, Arrays.asList("julia@example.com"))
          .and(phonesTag, Arrays.asList())));
    // [END CoGroupByKeyTupleOutputs]

    PCollection<String> actualFormattedResults =
        Snippets.coGroupByKeyTuple(emailsTag, phonesTag, emails, phones);

    // [START CoGroupByKeyTupleFormattedOutputs]
    final List<String> formattedResults = Arrays.asList(
        "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
        "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
        "james; []; ['222-333-4444']",
        "julia; ['julia@example.com']; []");
    // [END CoGroupByKeyTupleFormattedOutputs]

    // Make sure that both 'expectedResults' and 'actualFormattedResults' match with the
    // 'formattedResults'. 'expectedResults' will have to be formatted before comparing
    List<String> expectedFormattedResultsList = new ArrayList<>(expectedResults.size());
    for (KV<String, CoGbkResult> e : expectedResults) {
      String name = e.getKey();
      Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
      Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
      String formattedResult = Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
      expectedFormattedResultsList.add(formattedResult);
    }
    PCollection<String> expectedFormattedResultsPColl =
        p.apply(Create.of(expectedFormattedResultsList));
    PAssert.that(expectedFormattedResultsPColl).containsInAnyOrder(formattedResults);
    PAssert.that(actualFormattedResults).containsInAnyOrder(formattedResults);

    p.run();
  }
}
