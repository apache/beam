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

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class BigQueryIOJSONIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOJSONIT.class);

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private BigQueryIOJSONOptions options;

  private static String project;

  private static final String DATASET_ID = "bq_jsontype_test_nodelete";

  private static final String JSON_TYPE_TABLE_NAME = "json_data";

  private static String JSON_TABLE_DESTINATION;

  private static final List<KV<String, String>> JSON_TYPE_DATA = generateCountryData();

  // Convert PCollection of TableRows to a PCollection of KV JSON string pairs
  static class TableRowToJSONStringFn extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out){
      String country_code = row.get("country_code").toString();
      String country = row.get("country").toString();

      out.output(KV.of(country_code, country));
    }
  }

  static class CompareJSON implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    Map<String, String> expected;
    public CompareJSON(Map<String, String> expected){
      this.expected = expected;
    }

    // Compare PCollection input with the expected results.
    @Override
    public Void apply(Iterable<KV<String, String>> input) throws RuntimeException {
      LOG.info("Here in CompareJSON");
      int counter = 0;

      // Iterate through input list and convert each String to JsonElement and
      // compare with expected result JsonElements
      for(KV<String, String> entry: input){
        String key = entry.getKey();

        if(!expected.containsKey(key)){
          throw new NoSuchElementException(String.format(
              "Unexpected key '%s' found in input but does not exist in expected results.", key));
        }
        String jsonStringActual = entry.getValue();
        JsonElement jsonActual = JsonParser.parseString(jsonStringActual);

        String jsonStringExpected = expected.get(key);
        JsonElement jsonExpected = JsonParser.parseString(jsonStringExpected);

        System.out.println(key);
        System.out.println(jsonActual.toString());
        System.out.println(jsonExpected.toString());

        assertEquals(jsonExpected, jsonActual);
        counter += 1;
      }
      if(counter != expected.size()){
        throw new RuntimeException(String.format(
            "Expected %d elements but got %d elements.", expected.size(), counter));
      }
      return null;
    }
  }

  // reads TableRows from BigQuery and checks their JSON objects against an expected result.
  public void readAndValidateRows(BigQueryIOJSONOptions options, List<KV<String, String>> expectedResults){
    TypedRead<TableRow> bigqueryIO =
        BigQueryIO.readTableRows().withMethod(options.getReadMethod());

    // read from input query or from a table
    if(!options.getInputQuery().isEmpty()) {
      bigqueryIO = bigqueryIO.fromQuery(options.getInputQuery());
    } else {
      bigqueryIO = bigqueryIO.from(options.getInput());
    }

    PCollection<KV<String, String>> jsonKVPairs = p
        .apply("Read rows", bigqueryIO)
        .apply("Convert to KV JSON Strings", ParDo.of(new TableRowToJSONStringFn()));

    Map<String, String> expectedJsonResults = new HashMap<String, String>();
    for(KV<String, String> m: expectedResults){
      expectedJsonResults.put(m.getKey(), m.getValue());
    }

    PAssert.that(jsonKVPairs).satisfies(new CompareJSON(expectedJsonResults));

    p.run().waitUntilFinish();
  }

  @Test
  public void testDirectRead() throws Exception {
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJSONOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setInput(JSON_TABLE_DESTINATION);
    // options.setRunner(DataflowRunner.class);

    System.out.println("MY TABLE: " + JSON_TABLE_DESTINATION);

    readAndValidateRows(options, JSON_TYPE_DATA);
  }

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(BigQueryIOJSONOptions.class);
    // project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    // project = "apache-beam-testing";
    project = "google.com:clouddfe";

    JSON_TABLE_DESTINATION = String.format("%s:%s.%s", project, DATASET_ID, JSON_TYPE_TABLE_NAME);
  }

  public interface BigQueryIOJSONOptions extends TestPipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Query used to read from BigQuery")
    @Default.String("")
    String getInputQuery();

    void setInputQuery(String query);

    @Description("Read method used to read from BigQuery")
    @Default.Enum("DIRECT_READ")
    TypedRead.Method getReadMethod();

    void setReadMethod(TypedRead.Method value);

    @Description(
        "BigQuery table to write to, specified as "
            + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);


    @Description("Write disposition to use to write to BigQuery")
    @Default.Enum("WRITE_TRUNCATE")
    BigQueryIO.Write.WriteDisposition getWriteDisposition();

    void setWriteDisposition(BigQueryIO.Write.WriteDisposition value);

    @Description("Create disposition to use to write to BigQuery")
    @Default.Enum("CREATE_IF_NEEDED")
    BigQueryIO.Write.CreateDisposition getCreateDisposition();

    void setCreateDisposition(BigQueryIO.Write.CreateDisposition value);

    @Description("Write method used to write to BigQuery")
    @Default.Enum("STORAGE_WRITE_API")
    BigQueryIO.Write.Method getWriteMethod();

    void setWriteMethod(BigQueryIO.Write.Method value);
  }

  private static List<KV<String, String>> generateCountryData(){
    // Data from World Bank as of 2020
    JSONObject usa = new JSONObject();

    JSONObject nyc = new JSONObject();
    nyc.put("name", "New York City");
    nyc.put("state", "NY");
    nyc.put("population", 8622357);
    JSONObject la = new JSONObject();
    la.put("name", "Los Angeles");
    la.put("state", "CA");
    la.put("population", 4085014);
    JSONObject chicago = new JSONObject();
    chicago.put("name", "Chicago");
    chicago.put("state", "IL");
    chicago.put("population", 2670406);

    JSONObject us_cities = new JSONObject();
    us_cities.put("nyc", nyc);
    us_cities.put("la", la);
    us_cities.put("chicago", chicago);

    JSONArray usa_leaders = new JSONArray();
    usa_leaders.put("Donald Trump");
    usa_leaders.put("Barack Obama");
    usa_leaders.put("George W. Bush");
    usa_leaders.put("Bill Clinton");

    usa.put("name", "United States of America");
    usa.put("population", 329484123);
    usa.put("cities", us_cities);
    usa.put("past_leaders", usa_leaders);
    usa.put("in_northern_hemisphere", true);

    JSONObject aus = new JSONObject();

    JSONObject sydney = new JSONObject();
    sydney.put("name", "Sydney");
    sydney.put("state", "New South Wales");
    sydney.put("population", 5367206);

    JSONObject melbourne = new JSONObject();
    melbourne.put("name", "Melbourne");
    melbourne.put("state", "Victoria");
    melbourne.put("population", 5159211);

    JSONObject brisbane = new JSONObject();
    brisbane.put("name", "Birsbane");
    brisbane.put("state", "Queensland");
    brisbane.put("population", 2560720);

    JSONObject aus_cities = new JSONObject();
    aus_cities.put("sydney", sydney);
    aus_cities.put("melbourne", melbourne);
    aus_cities.put("brisbane", brisbane);

    JSONArray aus_leaders = new JSONArray();
    aus_leaders.put("Malcolm Turnbull");
    aus_leaders.put("Tony Abbot");
    aus_leaders.put("Kevin Rudd");

    aus.put("name", "Australia");
    aus.put("population", 25687041);
    aus.put("cities", aus_cities);
    aus.put("past_leaders", aus_leaders);
    aus.put("in_northern_hemisphere", false);


    JSONObject special = new JSONObject();

    JSONArray special_arr = new JSONArray();

    special_arr.put("1");
    special_arr.put("2");
    special_arr.put("!@#$%^&*()_+");

    special.put("name", "newline\n, form\f, tab\t, \"quotes\", \\backslash\\, backspace\b, \u0000_hex_\uf000");
    special.put("population", -123456789);
    special.put("cities", JSONObject.NULL);
    special.put("past_leaders", special_arr);
    special.put("in_northern_hemisphere", true);

    return ImmutableList.of(
        KV.of("usa", usa.toString()),
        KV.of("aus", aus.toString()),
        KV.of("special", special.toString()));
  }
}
