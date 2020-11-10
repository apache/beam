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
package org.apache.beam.examples.snippets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Snippets. */
@RunWith(JUnit4.class)
public class SnippetsTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testModelBigQueryIO() {
    // We cannot test BigQueryIO functionality in unit tests, therefore we limit ourselves
    // to making sure the pipeline containing BigQuery sources and sinks can be built.
    //
    // To run locally, set `runLocally` to `true`. You will have to set `project`, `dataset` and
    // `table` to the BigQuery table the test will write into.
    boolean runLocally = false;
    if (runLocally) {
      String project = "my-project";
      String dataset = "samples"; // this must already exist
      String table = "modelBigQueryIO"; // this will be created if needed

      BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
      options.setProject(project);
      options.setTempLocation("gs://" + project + "/samples/temp/");
      Pipeline p = Pipeline.create(options);
      Snippets.modelBigQueryIO(p, project, dataset, table);
      p.run();
    } else {
      Pipeline p = Pipeline.create();
      Snippets.modelBigQueryIO(p);
    }
  }

  /* Tests CoGroupByKeyTuple */
  @Test
  public void testCoGroupByKeyTuple() throws IOException {
    // [START CoGroupByKeyTupleInputs]
    final List<KV<String, String>> emailsList =
        Arrays.asList(
            KV.of("amy", "amy@example.com"),
            KV.of("carl", "carl@example.com"),
            KV.of("julia", "julia@example.com"),
            KV.of("carl", "carl@email.com"));

    final List<KV<String, String>> phonesList =
        Arrays.asList(
            KV.of("amy", "111-222-3333"),
            KV.of("james", "222-333-4444"),
            KV.of("amy", "333-444-5555"),
            KV.of("carl", "444-555-6666"));

    PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
    PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));
    // [END CoGroupByKeyTupleInputs]

    // [START CoGroupByKeyTupleOutputs]
    final TupleTag<String> emailsTag = new TupleTag<>();
    final TupleTag<String> phonesTag = new TupleTag<>();

    final List<KV<String, CoGbkResult>> expectedResults =
        Arrays.asList(
            KV.of(
                "amy",
                CoGbkResult.of(emailsTag, Arrays.asList("amy@example.com"))
                    .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))),
            KV.of(
                "carl",
                CoGbkResult.of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
                    .and(phonesTag, Arrays.asList("444-555-6666"))),
            KV.of(
                "james",
                CoGbkResult.of(emailsTag, Arrays.asList())
                    .and(phonesTag, Arrays.asList("222-333-4444"))),
            KV.of(
                "julia",
                CoGbkResult.of(emailsTag, Arrays.asList("julia@example.com"))
                    .and(phonesTag, Arrays.asList())));
    // [END CoGroupByKeyTupleOutputs]

    PCollection<String> actualFormattedResults =
        Snippets.coGroupByKeyTuple(emailsTag, phonesTag, emails, phones);

    // [START CoGroupByKeyTupleFormattedOutputs]
    final List<String> formattedResults =
        Arrays.asList(
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

  /* Tests SchemaJoinPattern */
  @Test
  public void testSchemaJoinPattern() {
    // [START SchemaJoinPatternCreate]
    // Define Schemas
    Schema emailSchema =
        Schema.of(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("email", Schema.FieldType.STRING));

    Schema phoneSchema =
        Schema.of(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("phone", Schema.FieldType.STRING));

    // Create User Data Collections
    final List<Row> emailUsers =
        Arrays.asList(
            Row.withSchema(emailSchema).addValue("person1").addValue("person1@example.com").build(),
            Row.withSchema(emailSchema).addValue("person2").addValue("person2@example.com").build(),
            Row.withSchema(emailSchema).addValue("person3").addValue("person3@example.com").build(),
            Row.withSchema(emailSchema).addValue("person4").addValue("person4@example.com").build(),
            Row.withSchema(emailSchema)
                .addValue("person6")
                .addValue("person6@example.com")
                .build());

    final List<Row> phoneUsers =
        Arrays.asList(
            Row.withSchema(phoneSchema).addValue("person1").addValue("111-222-3333").build(),
            Row.withSchema(phoneSchema).addValue("person2").addValue("222-333-4444").build(),
            Row.withSchema(phoneSchema).addValue("person3").addValue("444-333-4444").build(),
            Row.withSchema(phoneSchema).addValue("person4").addValue("555-333-4444").build(),
            Row.withSchema(phoneSchema).addValue("person5").addValue("777-333-4444").build());

    // [END SchemaJoinPatternCreate]

    PCollection<String> actualFormattedResult =
        Snippets.SchemaJoinPattern.main(p, emailUsers, phoneUsers, emailSchema, phoneSchema);

    final List<String> formattedResults =
        Arrays.asList(
            "Name: person1 Email: person1@example.com Phone: 111-222-3333",
            "Name: person2 Email: person2@example.com Phone: 222-333-4444",
            "Name: person3 Email: person3@example.com Phone: 444-333-4444",
            "Name: person4 Email: person4@example.com Phone: 555-333-4444");

    List<String> expectedFormattedResultsList = new ArrayList<>(formattedResults.size());

    for (int i = 0; i < formattedResults.size(); ++i) {
      String userInfo =
          "Name: "
              + emailUsers.get(i).getValue("name")
              + " Email: "
              + emailUsers.get(i).getValue("email")
              + " Phone: "
              + phoneUsers.get(i).getValue("phone");
      expectedFormattedResultsList.add(userInfo);
    }

    PCollection<String> expectedFormattedResultPcoll =
        p.apply(Create.of(expectedFormattedResultsList));

    PAssert.that(expectedFormattedResultPcoll).containsInAnyOrder(formattedResults);
    PAssert.that(actualFormattedResult).containsInAnyOrder(formattedResults);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesStatefulParDo.class})
  public void testSlowlyUpdatingSideInputsWindowed() {
    Instant startAt = Instant.now().minus(Duration.standardMinutes(3));
    Duration duration = Duration.standardSeconds(10);
    Instant stopAt = startAt.plus(duration);
    Duration interval1 = Duration.standardSeconds(1);
    Duration interval2 = Duration.standardSeconds(1);

    File f = null;
    try {
      f = File.createTempFile("testSlowlyUpdatingSIWindowed", "txt");
      try (BufferedWriter fw = Files.newWriter(f, Charset.forName("UTF-8"))) {
        fw.append("testdata");
      }
    } catch (IOException e) {
      Assert.fail("failed to create temp file: " + e.toString());
      throw new RuntimeException("Should never reach here");
    }

    PCollection<Long> result =
        Snippets.PeriodicallyUpdatingSideInputs.main(
            p, startAt, stopAt, interval1, interval2, f.getPath());

    ArrayList<Long> expectedResults = new ArrayList<Long>();
    expectedResults.add(0L);
    for (Long i = startAt.getMillis(); i < stopAt.getMillis(); i = i + interval2.getMillis()) {
      expectedResults.add(1L);
    }

    PAssert.that(result).containsInAnyOrder(expectedResults);

    p.run().waitUntilFinish();
    f.deleteOnExit();
  }
}
