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
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
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
public class BigQueryIOJsonTest {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOJsonTest.class);

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Rule
  public transient TestPipeline p_write = TestPipeline.create();

  private BigQueryIOJsonOptions options;

  private static final String project = "apache-beam-testing";
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

  public static final String STREAMING_TEST_TABLE = "streaming_test"
      + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  private static final Map<String, Map<String, Object>> JSON_TYPE_DATA = generateCountryData();

  // Convert PCollection of TableRows to a PCollection of KV JSON string pairs
  static class TableRowToKVJsonPairs extends DoFn<TableRow, KV<String, String>> {
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

  public void runTestWrite(BigQueryIOJsonOptions options){
    List<TableRow> rowsToWrite = new ArrayList<>();
    for(Map.Entry<String, Map<String, Object>> element: JSON_TYPE_DATA.entrySet()){
      rowsToWrite.add(new TableRow()
          .set("country_code", element.getKey())
          .set("country", element.getValue().get("country")));
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

    Map<String, String> expected = new HashMap<>();
    for(Map.Entry<String, Map<String, Object>> country : JSON_TYPE_DATA.entrySet()){
      expected.put(country.getKey(), country.getValue().get("country").toString());
    }
    readAndValidateRows(options, expected);
  }

  // reads TableRows from BigQuery and validates JSON Strings
  // expectedJsonResults Strings must be in valid json format
  public void readAndValidateRows(BigQueryIOJsonOptions options, Map<String, String> expectedResults){
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
        .apply("Convert to KV JSON Strings", ParDo.of(new TableRowToKVJsonPairs()));

    PAssert.that(jsonKVPairs).satisfies(new CompareJSON(expectedResults));

    p.run().waitUntilFinish();
  }

  @Test
  public void testDirectRead() throws Exception {
    LOG.info("Testing DIRECT_READ read method with JSON data");
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setInput(JSON_TABLE_DESTINATION);

    Map<String, String> expected = new HashMap<>();
    for(Map.Entry<String, Map<String, Object>> country : JSON_TYPE_DATA.entrySet()){
      expected.put(country.getKey(), country.getValue().get("country").toString());
    }

    readAndValidateRows(options, expected);  }

  @Test
  public void testExportRead() throws Exception {
    LOG.info("Testing EXPORT read method with JSON data");
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setReadMethod(TypedRead.Method.EXPORT);
    options.setInput(JSON_TABLE_DESTINATION);

    Map<String, String> expected = new HashMap<>();
    for(Map.Entry<String, Map<String, Object>> country : JSON_TYPE_DATA.entrySet()){
      expected.put(country.getKey(), country.getValue().get("country").toString());
    }

    readAndValidateRows(options, expected);  }

  @Test
  public void testQueryRead() throws Exception {
    LOG.info("Testing querying JSON data with DIRECT_READ read method");

    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setQuery(
        String.format("SELECT country_code, country.cities AS country FROM "
            + "`%s.%s.%s`", project, DATASET_ID, JSON_TYPE_TABLE_NAME));

    // get nested json objects from static data
    Map<String, String> expected = new HashMap<>();

    for(Map.Entry<String, Map<String, Object>> country : JSON_TYPE_DATA.entrySet()){
      expected.put(country.getKey(), country.getValue().get("cities").toString());
    }

    readAndValidateRows(options, expected);
  }

  @Test
  public void testStorageWrite() throws Exception{
    LOG.info("Testing writing JSON data with Storage API");

    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setWriteMethod(Write.Method.STORAGE_WRITE_API);

    String storage_destination = String.format("%s:%s.%s", project, DATASET_ID, STORAGE_WRITE_TEST_TABLE);
    options.setOutput(storage_destination);
    options.setInput(storage_destination);

    runTestWrite(options);
  }

  @Test
  public void testLegacyStreamingWrite() throws Exception{
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setWriteMethod(Write.Method.STREAMING_INSERTS);

    String streaming_destination = String.format("%s:%s.%s", project, DATASET_ID, STREAMING_TEST_TABLE);
    options.setOutput(streaming_destination);
    options.setInput(streaming_destination);

    runTestWrite(options);
  }

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(BigQueryIOJsonOptions.class);

    JSON_TABLE_DESTINATION = String.format("%s:%s.%s", project, DATASET_ID, JSON_TYPE_TABLE_NAME);
  }

  public interface BigQueryIOJsonOptions extends TestPipelineOptions {
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

  private static Map<String, Map<String, Object>> generateCountryData(){
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

    JSONObject usa_cities = new JSONObject();
    usa_cities.put("nyc", nyc);
    usa_cities.put("la", la);
    usa_cities.put("chicago", chicago);

    JSONArray usa_leaders = new JSONArray();
    usa_leaders.put("Donald Trump");
    usa_leaders.put("Barack Obama");
    usa_leaders.put("George W. Bush");
    usa_leaders.put("Bill Clinton");

    usa.put("name", "United States of America");
    usa.put("population", 329484123);
    usa.put("cities", usa_cities);
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

    JSONObject special_cities = new JSONObject();

    JSONObject ba_sing_se = new JSONObject();
    ba_sing_se.put("name", "Ba Sing Se");
    ba_sing_se.put("state", "The Earth Kingdom");
    ba_sing_se.put("population", 200000);

    JSONObject bikini_bottom = new JSONObject();
    bikini_bottom.put("name", "Bikini Bottom");
    ba_sing_se.put("state", "The Pacific Ocean");
    ba_sing_se.put("population", 50000);

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

    // usa landmarks
    JSONObject statue_of_liberty = new JSONObject();
    statue_of_liberty.put("name", "Statue of Liberty");
    statue_of_liberty.put("cool rating", JSONObject.NULL);
    JSONObject golden_gate_bridge = new JSONObject();
    golden_gate_bridge.put("name", "Golden Gate Bridge");
    golden_gate_bridge.put("cool rating", "very cool");
    JSONObject grand_canyon = new JSONObject();
    grand_canyon.put("name", "Grand Canyon");
    grand_canyon.put("cool rating", "very very cool");

    // australia landmarks
    JSONObject opera_house = new JSONObject();
    opera_house.put("name", "Sydney Opera House");
    opera_house.put("cool rating", "amazing");
    JSONObject great_barrier_reef = new JSONObject();
    great_barrier_reef.put("name", "Great Barrier Reef");
    great_barrier_reef.put("cool rating", JSONObject.NULL);


    // special landmarks
    JSONObject hogwarts = new JSONObject();
    hogwarts.put("name", "Hogwarts School of WitchCraft and Wizardry");
    hogwarts.put("cool rating", "magical");
    JSONObject willy_wonka = new JSONObject();
    willy_wonka.put("name", "Willy Wonka's Factory");
    willy_wonka.put("cool rating", JSONObject.NULL);
    JSONObject rivendell = new JSONObject();
    rivendell.put("name", "Rivendell");
    rivendell.put("cool rating", "precious");

    // stats
    JSONObject us_gdp = new JSONObject();
    us_gdp.put("gdp_per_capita", 58559.675);
    us_gdp.put("currency", "constant 2015 US$");
    JSONObject us_co2 = new JSONObject();
    us_co2.put("co2 emissions", 15.241);
    us_co2.put("measurement", "metric tons per capita");
    us_co2.put("year", 2018);

    JSONObject aus_gdp = new JSONObject();
    aus_gdp.put("gdp_per_capita", 58043.581);
    aus_gdp.put("currency", "constant 2015 US$");
    JSONObject aus_co2 = new JSONObject();
    aus_co2.put("co2 emissions", 15.476);
    aus_co2.put("measurement", "metric tons per capita");
    aus_co2.put("year", 2018);

    JSONObject special_gdp = new JSONObject();
    special_gdp.put("gdp_per_capita", 421.70);
    special_gdp.put("currency", "constant 200 BC gold");
    JSONObject special_co2 = new JSONObject();
    special_co2.put("co2 emissions", -10.79);
    special_co2.put("measurement", "metric tons per capita");
    special_co2.put("year", 2018);



    return ImmutableMap.of(
        // keys for testing
        "countries", ImmutableMap.of(
            "usa", usa.toString(),
            "aus", aus.toString(),
            "special", special.toString()
        ),
        "cities", ImmutableMap.of(
            "usa_nyc", nyc.toString(),
            "usa_la", la.toString(),
            "usa_chicago", chicago.toString(),
            "aus_sydney", sydney.toString(),
            "aus_melbourne", melbourne.toString(),
            "aus_brisbane", brisbane.toString(),
            "special_basingse", ba_sing_se.toString(),
            "special_bikinibottom", bikini_bottom.toString()
        ),
        "landmarks", ImmutableMap.of(
            "usa_0", statue_of_liberty.toString(),
            "usa_1", golden_gate_bridge.toString(),
            "usa_2", grand_canyon.toString(),
            "aus_0", opera_house.toString(),
            "aus_1", great_barrier_reef.toString(),
            "special_0", hogwarts.toString(),
            "special_1", willy_wonka.toString(),
            "special_2", rivendell.toString()
        ),
        "stats", ImmutableMap.of(
            "usa_gdp_per_capita", us_gdp.toString(),
            "usa_co2_emissions", us_co2.toString(),
            "aus_gdp_per_capita", aus_gdp.toString(),
            "aus_co2_emissions", aus_co2.toString(),
            "special_gdp_per_capita", special_gdp.toString(),
            "special_co2_emissions", special_co2.toString()
        ),
        // keys for writing to BigQuery
        "usa", ImmutableMap.of(
            "country", usa.toString(),
            "cities", ImmutableMap.of(
                "nyc", nyc.toString(),
                "la", la.toString(),
                "chicago", chicago.toString()
            ),
            "landmarks", Arrays.asList(statue_of_liberty.toString(),
                golden_gate_bridge.toString(), grand_canyon.toString()),
            "stats", ImmutableMap.of(
                "gdp_per_capita", us_gdp.toString(),
                "co2_emissions", us_co2.toString()
            )
        ),
        "aus", ImmutableMap.of(
            "country", aus.toString(),
            "cities", ImmutableMap.of(
                "sydney", sydney.toString(),
                "melbourne", melbourne.toString(),
                "brisbane", brisbane.toString()
            ),
            "landmarks", Arrays.asList(opera_house.toString(), great_barrier_reef.toString()),
            "stats", ImmutableMap.of(
                "gdp_per_capita", aus_gdp.toString(),
                "co2_emissions", aus_co2.toString()
            )
        ),
        "special", ImmutableMap.of(
            "country", special.toString(),
            "cities", ImmutableMap.of(
                "basingse", ba_sing_se.toString(),
                "bikinibottom", bikini_bottom.toString()
            ),
            "landmarks", Arrays.asList(hogwarts.toString(),
                willy_wonka.toString(), rivendell.toString()),
            "stats", ImmutableMap.of(
                "gdp_per_capita", special_gdp.toString(),
                "co2_emissions", special_co2.toString()
            )
        )
    );
  }
}
