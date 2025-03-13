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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StorageApiSinkDefaultValuesIT {
  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiSinkDefaultValuesIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_default_values" + System.nanoTime();

  private static String bigQueryLocation;

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    bigQueryLocation =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation();
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID, null, bigQueryLocation);
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  private static String createAndGetTablespec(TableSchema tableSchema)
      throws IOException, InterruptedException {
    String tableName = "table" + System.nanoTime();
    TableReference tableReference =
        new TableReference()
            .setProjectId(PROJECT)
            .setDatasetId(BIG_QUERY_DATASET_ID)
            .setTableId(tableName);
    BQ_CLIENT.createNewTable(
        PROJECT,
        BIG_QUERY_DATASET_ID,
        new Table().setSchema(tableSchema).setTableReference(tableReference));
    return PROJECT + "." + BIG_QUERY_DATASET_ID + "." + tableName;
  }

  @Test
  public void testMissingValueSchemaKnownTakeDefault() throws IOException, InterruptedException {
    runTest(true, true, false);
  }

  @Test
  public void testMissingRequiredValueSchemaKnownTakeDefault()
      throws IOException, InterruptedException {
    runTest(true, true, true);
  }

  @Test
  public void testMissingRequiredValueSchemaKnownTakeNull()
      throws IOException, InterruptedException {
    runTest(true, false, true);
  }

  @Test
  public void testMissingRequiredValueSchemaUnknownTakeDefault()
      throws IOException, InterruptedException {
    runTest(false, true, true);
  }

  @Test
  public void testMissingValueSchemaUnknownTakeDefault() throws IOException, InterruptedException {

    runTest(false, true, false);
  }

  @Test
  public void testMissingValueSchemaKnownTakeNull() throws IOException, InterruptedException {
    runTest(true, false, false);
  }

  @Test
  @Ignore // This currently appears broke in BigQuery.
  public void testMissingValueSchemaUnknownTakeNull() throws IOException, InterruptedException {
    runTest(false, false, false);
  }

  public void runTest(
      boolean sinkKnowsDefaultFields, boolean takeDefault, boolean defaultFieldsRequired)
      throws IOException, InterruptedException {
    boolean expectDeadLetter = !takeDefault && defaultFieldsRequired;
    TableSchema bqSchema;
    if (defaultFieldsRequired) {
      bqSchema =
          new TableSchema()
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema().setName("id").setType("STRING"),
                      new TableFieldSchema().setName("key2").setType("STRING"),
                      new TableFieldSchema().setName("value").setType("STRING"),
                      new TableFieldSchema()
                          .setName("defaultrepeated")
                          .setType("STRING")
                          .setMode("REPEATED")
                          .setDefaultValueExpression("['a','b', 'c']"),
                      new TableFieldSchema()
                          .setName("defaultliteral")
                          .setType("INT64")
                          .setDefaultValueExpression("42")
                          .setMode("REQUIRED"),
                      new TableFieldSchema()
                          .setName("defaulttime")
                          .setType("TIMESTAMP")
                          .setDefaultValueExpression("CURRENT_TIMESTAMP()")
                          .setMode("REQUIRED")));
    } else {
      bqSchema =
          new TableSchema()
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema().setName("id").setType("STRING"),
                      new TableFieldSchema().setName("key2").setType("STRING"),
                      new TableFieldSchema().setName("value").setType("STRING"),
                      new TableFieldSchema()
                          .setName("defaultrepeated")
                          .setType("STRING")
                          .setMode("REPEATED")
                          .setDefaultValueExpression("['a','b', 'c']"),
                      new TableFieldSchema()
                          .setName("defaultliteral")
                          .setType("INT64")
                          .setDefaultValueExpression("42"),
                      new TableFieldSchema()
                          .setName("defaulttime")
                          .setType("TIMESTAMP")
                          .setDefaultValueExpression("CURRENT_TIMESTAMP()")));
    }

    TableSchema sinkSchema = bqSchema;
    if (!sinkKnowsDefaultFields) {
      sinkSchema =
          new TableSchema()
              .setFields(
                  bqSchema.getFields().stream()
                      .filter(tfs -> tfs.getDefaultValueExpression() == null)
                      .collect(Collectors.toList()));
    }
    final TableRow row1 =
        new TableRow()
            .set("id", "row1")
            .set("key2", "bar0")
            .set("value", "1")
            .set("defaultliteral", 12)
            .set("defaultrepeated", Lists.newArrayList("foo", "bar"));
    final TableRow row2 = new TableRow().set("id", "row2").set("key2", "bar1").set("value", "1");
    final TableRow row3 = new TableRow().set("id", "row3").set("key2", "bar2").set("value", "2");

    List<TableRow> tableRows = Lists.newArrayList(row1, row2, row3);

    String tableSpec = createAndGetTablespec(bqSchema);
    Pipeline p = Pipeline.create();

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withSchema(sinkSchema)
            .withNumStorageWriteApiStreams(2)
            .ignoreUnknownValues()
            .withTriggeringFrequency(Duration.standardSeconds(1))
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER);
    if (!takeDefault) {
      write =
          write.withDefaultMissingValueInterpretation(
              AppendRowsRequest.MissingValueInterpretation.NULL_VALUE);
    }
    WriteResult writeResult =
        p.apply("Create rows", Create.of(tableRows))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
            .apply("write", write);
    if (expectDeadLetter) {
      PAssert.that(writeResult.getFailedStorageApiInserts())
          .satisfies(
              (SerializableFunction<Iterable<BigQueryStorageApiInsertError>, Void>)
                  input -> {
                    assertThat(Lists.newArrayList(input).size(), is(3));
                    //     assertThat(input, containsInAnyOrder(tableRows));
                    return null;
                  });
    }
    p.run();

    if (!expectDeadLetter) {
      Map<String, TableRow> queryResponse =
          BQ_CLIENT
              .queryUnflattened(
                  String.format("SELECT * FROM %s", tableSpec),
                  PROJECT,
                  true,
                  true,
                  bigQueryLocation)
              .stream()
              .collect(Collectors.toMap(tr -> (String) tr.get("id"), Function.identity()));
      assertThat(queryResponse.size(), equalTo(3));

      TableRow resultRow1 = Preconditions.checkArgumentNotNull(queryResponse.get("row1"));
      TableRow resultRow2 = Preconditions.checkArgumentNotNull(queryResponse.get("row2"));
      TableRow resultRow3 = Preconditions.checkArgumentNotNull(queryResponse.get("row3"));

      if (sinkKnowsDefaultFields) {
        assertThat(resultRow1.get("defaultliteral"), equalTo("12"));
        assertThat(
            (Collection<String>) resultRow1.get("defaultrepeated"),
            containsInAnyOrder("foo", "bar"));
        if (takeDefault) {
          assertNotNull(resultRow1.get("defaulttime"));
          assertNotNull(resultRow2.get("defaulttime"));
          assertThat(resultRow2.get("defaultliteral"), equalTo("42"));
          assertThat(
              (Collection<String>) resultRow2.get("defaultrepeated"),
              containsInAnyOrder("a", "b", "c"));
          assertNotNull(resultRow3.get("defaulttime"));
          assertThat(resultRow3.get("defaultliteral"), equalTo("42"));
          assertThat(
              (Collection<String>) resultRow3.get("defaultrepeated"),
              containsInAnyOrder("a", "b", "c"));
        } else {
          assertNull(resultRow1.get("defaulttime"));
          assertNull(resultRow2.get("defaulttime"));
          assertNull(resultRow2.get("defaultliteral"));
          assertThat((Collection<String>) resultRow2.get("defaultrepeated"), Matchers.empty());
          assertNull(resultRow3.get("defaulttime"));
          assertNull(resultRow3.get("defaultliteral"));
          assertThat((Collection<String>) resultRow3.get("defaultrepeated"), Matchers.empty());
        }
      } else {
        if (takeDefault) {
          assertNotNull(resultRow1.get("defaulttime"));
          assertThat(resultRow1.get("defaultliteral"), equalTo("42"));
          assertThat(
              (Collection<String>) resultRow1.get("defaultrepeated"),
              containsInAnyOrder("a", "b", "c"));
          assertNotNull(resultRow2.get("defaulttime"));
          assertThat(resultRow2.get("defaultliteral"), equalTo("42"));
          assertThat(
              (Collection<String>) resultRow2.get("defaultrepeated"),
              containsInAnyOrder("a", "b", "c"));
          assertNotNull(resultRow3.get("defaulttime"));
          assertThat(resultRow3.get("defaultliteral"), equalTo("42"));
          assertThat(
              (Collection<String>) resultRow3.get("defaultrepeated"),
              containsInAnyOrder("a", "b", "c"));
        } else {
          assertNull(resultRow1.get("defaulttime"));
          assertNull(resultRow1.get("defaultliteral"));
          assertThat((Collection<String>) resultRow1.get("defaultrepeated"), Matchers.empty());
          assertNull(resultRow2.get("defaulttime"));
          assertNull(resultRow2.get("defaultliteral"));
          assertThat((Collection<String>) resultRow2.get("defaultrepeated"), Matchers.empty());
          assertNull(resultRow3.get("defaulttime"));
          assertNull(resultRow3.get("defaultliteral"));
          assertThat((Collection<String>) resultRow3.get("defaultrepeated"), Matchers.empty());
        }
      }
    }
  }
}
