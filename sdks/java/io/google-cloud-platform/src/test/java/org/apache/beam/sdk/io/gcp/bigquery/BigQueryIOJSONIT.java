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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

  @Rule
  public transient TestPipeline p_write = TestPipeline.create();

  private BigQueryIOJSONOptions options;

  private static String project;

  private static final String DATASET_ID = "bq_jsontype_test_nodelete";

  private static final String JSON_TYPE_TABLE_NAME = "json_data";

  private static String JSON_TABLE_DESTINATION;

  private static final TableSchema JSON_TYPE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(ImmutableList.of(
              new TableFieldSchema().setName("country_code").setType("STRING"),
              new TableFieldSchema().setName("country").setType("JSON")
          ));

  public static final String STORAGE_WRITE_TEST_TABLE = "storagewrite_test"
      + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  private static final Map<String, String> JSON_TYPE_DATA = generateCountryData(false);

  // Convert PCollection of TableRows to a PCollection of KV JSON string pairs
  static class TableRowToJSONStringFn extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out){
      String country_code = row.get("country_code").toString();
      String country = row.get("country").toString();

      out.output(KV.of(country_code, country));
    }
  }

  // Compare PCollection input with expected results.
  static class CompareJSON implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    Map<String, String> expected;
    public CompareJSON(Map<String, String> expected){
      this.expected = expected;
    }

    @Override
    public Void apply(Iterable<KV<String, String>> input) throws RuntimeException {
      int counter = 0;

      // Iterate through input list and convert each String to JsonElement
      // Compare with expected result JsonElements
      for(KV<String, String> actual: input){
        String key = actual.getKey();

        if(!expected.containsKey(key)){
          throw new NoSuchElementException(String.format(
              "Unexpected key '%s' found in input but does not exist in expected results.", key));
        }
        String jsonStringActual = actual.getValue();
        JsonElement jsonActual = JsonParser.parseString(jsonStringActual);

        String jsonStringExpected = expected.get(key);
        JsonElement jsonExpected = JsonParser.parseString(jsonStringExpected);

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

  public void runTestWrite(BigQueryIOJSONOptions options){
    List<TableRow> rowsToWrite = new ArrayList<>();
    for(Map.Entry<String, String> element: JSON_TYPE_DATA.entrySet()){
      rowsToWrite.add(new TableRow()
          .set("country_code", element.getKey())
          .set("country", element.getValue()));
    }

    p_write
        .apply("Create Elements", Create.of(rowsToWrite))
        .apply("Write To BigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getOutput())
                .withSchema(JSON_TYPE_TABLE_SCHEMA)
                .withCreateDisposition(options.getCreateDisposition())
                .withMethod(options.getWriteMethod()));
    p_write.run().waitUntilFinish();

    options.setReadMethod(TypedRead.Method.EXPORT);
    readAndValidateRows(options, JSON_TYPE_DATA);
  }

  // reads TableRows from BigQuery and validates JSON Strings
  // expectedJsonResults Strings must be in valid json format
  public void readAndValidateRows(BigQueryIOJSONOptions options, Map<String, String> expectedResults){
    TypedRead<TableRow> bigqueryIO =
        BigQueryIO.readTableRows().withMethod(options.getReadMethod());

    // read from input query or from table
    if(!options.getQuery().isEmpty()) {
      bigqueryIO = bigqueryIO.fromQuery(options.getQuery()).usingStandardSql();
    } else {
      bigqueryIO = bigqueryIO.from(options.getInput());
    }

    PCollection<KV<String, String>> jsonKVPairs = p
        .apply("Read rows", bigqueryIO)
        .apply("Convert to KV JSON Strings", ParDo.of(new TableRowToJSONStringFn()));

    PAssert.that(jsonKVPairs).satisfies(new CompareJSON(expectedResults));

    p.run().waitUntilFinish();
  }

  @Test
  public void testDirectRead() throws Exception {
    LOG.info("Testing DIRECT_READ read method with JSON data");
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJSONOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setInput(JSON_TABLE_DESTINATION);

    readAndValidateRows(options, JSON_TYPE_DATA);
  }

  @Test
  public void testExportRead() throws Exception {
    LOG.info("Testing EXPORT read method with JSON data");
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJSONOptions.class);
    options.setReadMethod(TypedRead.Method.EXPORT);
    options.setInput(JSON_TABLE_DESTINATION);

    readAndValidateRows(options, JSON_TYPE_DATA);
  }

  @Test
  public void testQueryRead() throws Exception {
    LOG.info("Testing querying JSON data with DIRECT_READ read method");

    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJSONOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setQuery(
        String.format("SELECT country_code, country.cities AS country FROM "
            + "`%s.%s.%s`", project, DATASET_ID, JSON_TYPE_TABLE_NAME));

    // get nested json objects from static data
    Map<String, String> expected = generateCountryData(true);

    readAndValidateRows(options, expected);
  }

  @Test
  public void testStorageWrite() throws Exception{
    LOG.info("Testing writing JSON data with Storage API");

    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJSONOptions.class);
    options.setWriteMethod(Write.Method.STORAGE_WRITE_API);

    String storage_destination = String.format("%s:%s.%s", project, DATASET_ID, STORAGE_WRITE_TEST_TABLE);
    options.setOutput(storage_destination);
    options.setInput(storage_destination);

    runTestWrite(options);
  }

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(BigQueryIOJSONOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    JSON_TABLE_DESTINATION = String.format("%s:%s.%s", project, DATASET_ID, JSON_TYPE_TABLE_NAME);
  }

  public interface BigQueryIOJSONOptions extends TestPipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    String getInput();

    void setInput(String value);

    @Description("Query used to read from BigQuery")
    @Default.String("")
    String getQuery();

    void setQuery(String query);

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

  private static Map<String, String> generateCountryData(boolean isQuery){
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

    JSONObject ba_sing_se = new JSONObject();
    ba_sing_se.put("name", "Ba Sing Se");
    ba_sing_se.put("state", "The Earth Kingdom");
    ba_sing_se.put("population", 200000);
    JSONObject bikini_bottom = new JSONObject();
    bikini_bottom.put("name", "Bikini Bottom");
    ba_sing_se.put("state", "The Pacific Ocean");
    ba_sing_se.put("population", 50000);

    JSONObject special_cities = new JSONObject();
    special_cities.put("basingse", ba_sing_se);
    special_cities.put("bikinibottom", bikini_bottom);

    JSONArray special_arr = new JSONArray();

    special_arr.put("1");
    special_arr.put("2");
    special_arr.put("!@#$%^&*()_+");

    special.put("name", "newline\n, form\f, tab\t, \"quotes\", \\backslash\\, backspace\b, \u0000_hex_\u0f0f");
    special.put("population", -123456789);
    special.put("cities", special_cities);
    special.put("past_leaders", special_arr);
    special.put("in_northern_hemisphere", true);

    if(isQuery){
      Map<String, String> cities = new HashMap<>();
      cities.put("usa", us_cities.toString());
      cities.put("aus", aus_cities.toString());
      cities.put("special", special_cities.toString());

      return cities;
    }

    Map<String, String> countries = new HashMap<>();
    countries.put("usa", usa.toString());
    countries.put("aus", aus.toString());
    countries.put("special", special.toString());

    return countries;
  }
}
