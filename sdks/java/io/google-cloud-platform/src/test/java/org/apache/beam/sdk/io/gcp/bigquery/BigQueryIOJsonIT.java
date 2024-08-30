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
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Integration test for reading and writing JSON data to/from BigQuery */
@RunWith(JUnit4.class)
public class BigQueryIOJsonIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOJsonIT.class);

  private static PipelineOptions testOptions = TestPipeline.testingPipelineOptions();

  static {
    TestPipelineOptions opt = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    testOptions.setTempLocation(
        FileSystems.matchNewDirectory(opt.getTempRoot(), "java-tmp").toString());
  }

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(testOptions);

  @Rule public final transient TestPipeline pWrite = TestPipeline.fromOptions(testOptions);

  private BigQueryIOJsonOptions options;

  private static final String project = "apache-beam-testing";
  private static final String DATASET_ID = "bq_jsontype_test_nodelete";
  private static final String JSON_TABLE_NAME = "json_data";

  @SuppressWarnings("unused") // persistent test fixture, though unused for the moment
  private static final String JSON_TABLE_DESTINATION =
      String.format("%s:%s.%s", project, DATASET_ID, JSON_TABLE_NAME);

  private static final TableSchema JSON_TYPE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("country_code").setType("STRING"),
                  new TableFieldSchema().setName("country").setType("JSON"),
                  new TableFieldSchema()
                      .setName("stats")
                      .setType("STRUCT")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema().setName("gdp_per_capita").setType("JSON"),
                              new TableFieldSchema().setName("co2_emissions").setType("JSON"))),
                  new TableFieldSchema()
                      .setName("cities")
                      .setType("STRUCT")
                      .setMode("REPEATED")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema().setName("city_name").setType("STRING"),
                              new TableFieldSchema().setName("city").setType("JSON"))),
                  new TableFieldSchema().setName("landmarks").setType("JSON").setMode("REPEATED")));

  private static final List<TableRow> JSON_QUERY_TEST_DATA =
      Arrays.asList(
          new TableRow()
              .set("country_code", "usa")
              .set("past_leader", "\"George W. Bush\"")
              .set("gdp", "58559.675")
              .set("city_name", "\"Los Angeles\"")
              .set("landmark_name", "\"Golden Gate Bridge\""),
          new TableRow()
              .set("country_code", "aus")
              .set("past_leader", "\"Kevin Rudd\"")
              .set("gdp", "58043.581")
              .set("city_name", "\"Melbourne\"")
              .set("landmark_name", "\"Great Barrier Reef\""),
          new TableRow()
              .set("country_code", "special")
              .set("past_leader", "\"!@#$%^&*()_+\"")
              .set("gdp", "421.7")
              .set("city_name", "\"Bikini Bottom\"")
              .set("landmark_name", "\"Willy Wonka's Factory\""));

  public static final String STORAGE_WRITE_TEST_TABLE =
      "storagewrite_test" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  public static final String FILE_LOAD_TEST_TABLE =
      "fileload_test" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  public static final String STREAMING_TEST_TABLE =
      "streaming_test" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  private static final Map<String, Map<String, Object>> JSON_TEST_DATA = generateCountryData();

  // Make KV Json String pairs out of "country" column
  static class CountryToKVJsonString extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out) {
      String countryCode = row.get("country_code").toString();
      String country = row.get("country").toString();

      out.output(KV.of(countryCode, country));
    }
  }

  // Make KV Json String pairs out of "cities" column
  static class CitiesToKVJsonString extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out) {
      String countryCode = row.get("country_code").toString();
      ArrayList<Map<String, String>> cities = (ArrayList<Map<String, String>>) row.get("cities");

      for (Map<String, String> city : cities) {
        String key = countryCode + "_" + city.get("city_name");
        String value = city.get("city");

        out.output(KV.of(key, value));
      }
    }
  }

  // Make KV Json String pairs out of "stats" column
  static class StatsToKVJsonString extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out) {
      String countryCode = row.get("country_code").toString();
      Map<String, String> map = (Map<String, String>) row.get("stats");

      for (Map.Entry<String, String> entry : map.entrySet()) {
        String key = countryCode + "_" + entry.getKey();
        String value = entry.getValue();

        out.output(KV.of(key, value));
      }
    }
  }

  // Make KV Json String pairs out of "landmarks" column
  static class LandmarksToKVJsonString extends DoFn<TableRow, KV<String, String>> {
    @ProcessElement
    public void processElement(@Element TableRow row, OutputReceiver<KV<String, String>> out) {
      String countryCode = row.get("country_code").toString();
      ArrayList<String> landmarks = (ArrayList<String>) row.get("landmarks");

      for (int i = 0; i < landmarks.size(); i++) {
        String key = countryCode + "_" + i;
        String value = landmarks.get(i);

        out.output(KV.of(key, value));
      }
    }
  }

  // Compare PCollection input with expected results.
  static class CompareJsonStrings
      implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    Map<String, String> expected;
    // Unescape actual string or not. This is to handle (currently) inconsistent behavior of same
    // input data for different write methods.
    //
    // When feed a string to BigQuery JSON field, FILE_LOAD gives escaped string (e.g. "[\"a\"]" )
    // while other write methods (STORAGE_WRITE_API, STREAMING_INSET) convert it to JSON string
    // (e.g. ["a"])
    final boolean unescape;

    public CompareJsonStrings(Map<String, String> expected) {
      this(expected, false);
    }

    public CompareJsonStrings(Map<String, String> expected, boolean unescape) {
      this.expected = expected;
      this.unescape = unescape;
    }

    @Override
    public Void apply(Iterable<KV<String, String>> input) throws RuntimeException {
      int counter = 0;

      // Compare by converting strings to JsonElements
      for (KV<String, String> actual : input) {
        String key = actual.getKey();

        if (!expected.containsKey(key)) {
          throw new NoSuchElementException(
              String.format(
                  "Unexpected key '%s' found in input but does not exist in expected results.",
                  key));
        }
        String jsonStringActual = actual.getValue();

        // TODO(yathu) remove this conversion if FILE_LOAD produces unescaped JSON string
        if (unescape && jsonStringActual.length() > 1) {
          jsonStringActual =
              StringEscapeUtils.unescapeEcmaScript(
                  jsonStringActual.substring(1, jsonStringActual.length() - 1));
        }

        JsonElement jsonActual = JsonParser.parseString(jsonStringActual);

        String jsonStringExpected = expected.get(key);
        JsonElement jsonExpected = JsonParser.parseString(jsonStringExpected);

        assertEquals(jsonExpected, jsonActual);
        counter += 1;
      }
      if (counter != expected.size()) {
        throw new RuntimeException(
            String.format("Expected %d elements but got %d elements.", expected.size(), counter));
      }
      return null;
    }
  }

  // Writes with given write method then reads back and validates with original test data.
  public void runTestWriteRead(BigQueryIOJsonOptions options) {
    List<String> countries = Arrays.asList("usa", "aus", "special");
    List<TableRow> rowsToWrite = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> element : JSON_TEST_DATA.entrySet()) {
      if (!countries.contains(element.getKey())) {
        continue;
      }

      TableRow row =
          new TableRow()
              .set("country_code", element.getKey())
              .set("country", element.getValue().get("country"))
              .set("stats", element.getValue().get("stats"))
              .set("cities", element.getValue().get("cities"))
              .set("landmarks", element.getValue().get("landmarks"));

      rowsToWrite.add(row);
    }

    pWrite
        .apply("Create Elements", Create.of(rowsToWrite))
        .apply(
            "Write To BigQuery",
            BigQueryIO.writeTableRows()
                .to(options.getOutput())
                .withSchema(JSON_TYPE_TABLE_SCHEMA)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(options.getWriteMethod()));
    pWrite.run().waitUntilFinish();

    readAndValidateRows(options);
  }

  public static Map<String, String> getTestData(String column) {
    Map<String, String> testData =
        JSON_TEST_DATA.get(column).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
    return testData;
  }

  // Read from BigQuery and compare with local test data.
  public void readAndValidateRows(BigQueryIOJsonOptions options) {
    TypedRead<TableRow> bigqueryIO = BigQueryIO.readTableRows().withMethod(options.getReadMethod());

    // read from input query or from table
    if (!options.getInputQuery().isEmpty()) {
      bigqueryIO = bigqueryIO.fromQuery(options.getInputQuery()).usingStandardSql();
    } else {
      bigqueryIO = bigqueryIO.from(options.getInputTable());
    }

    PCollection<TableRow> jsonRows = p.apply("Read rows", bigqueryIO);

    if (!options.getInputQuery().isEmpty()) {
      PAssert.that(jsonRows).containsInAnyOrder(JSON_QUERY_TEST_DATA);
      p.run().waitUntilFinish();
      return;
    }

    final boolean unescape = options.getWriteMethod() == Write.Method.FILE_LOADS;

    // Testing countries (straight json)
    PCollection<KV<String, String>> countries =
        jsonRows.apply(
            "Convert countries to KV JSON Strings", ParDo.of(new CountryToKVJsonString()));

    PAssert.that(countries).satisfies(new CompareJsonStrings(getTestData("countries"), unescape));

    // Testing stats (json in struct)
    PCollection<KV<String, String>> stats =
        jsonRows.apply("Convert stats to KV JSON Strings", ParDo.of(new StatsToKVJsonString()));

    PAssert.that(stats).satisfies(new CompareJsonStrings(getTestData("stats"), unescape));

    // Testing cities (json in array of structs)
    PCollection<KV<String, String>> cities =
        jsonRows.apply("Convert cities to KV JSON Strings", ParDo.of(new CitiesToKVJsonString()));

    PAssert.that(cities).satisfies(new CompareJsonStrings(getTestData("cities"), unescape));

    // Testing landmarks (json in array)
    PCollection<KV<String, String>> landmarks =
        jsonRows.apply(
            "Convert landmarks to KV JSON Strings", ParDo.of(new LandmarksToKVJsonString()));

    PAssert.that(landmarks).satisfies(new CompareJsonStrings(getTestData("landmarks"), unescape));

    p.run().waitUntilFinish();
  }

  @Test
  public void testQueryRead() throws Exception {
    LOG.info("Testing querying JSON data with DIRECT_READ read method");
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);
    options.setInputQuery(
        String.format(
            "SELECT "
                + "country_code, "
                + "country.past_leaders[2] AS past_leader, "
                + "stats.gdp_per_capita[\"gdp_per_capita\"] AS gdp, "
                + "cities[OFFSET(1)].city.name AS city_name, "
                + "landmarks[OFFSET(1)][\"name\"] AS landmark_name FROM "
                + "`%s.%s.%s`",
            project, DATASET_ID, JSON_TABLE_NAME));

    readAndValidateRows(options);
  }

  @Test
  public void testStorageWriteRead() {
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setWriteMethod(Write.Method.STORAGE_WRITE_API);
    options.setReadMethod(TypedRead.Method.DIRECT_READ);

    String storageDestination =
        String.format("%s:%s.%s", project, DATASET_ID, STORAGE_WRITE_TEST_TABLE);
    options.setOutput(storageDestination);
    options.setInputTable(storageDestination);

    runTestWriteRead(options);
  }

  @Test
  public void testFileLoadWriteExportRead() {
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setWriteMethod(Write.Method.FILE_LOADS);
    options.setReadMethod(TypedRead.Method.EXPORT);

    String storageDestination =
        String.format("%s:%s.%s", project, DATASET_ID, FILE_LOAD_TEST_TABLE);
    options.setOutput(storageDestination);
    options.setInputTable(storageDestination);

    runTestWriteRead(options);
  }

  @Test
  public void testLegacyStreamingWriteDefaultRead() {
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOJsonOptions.class);
    options.setWriteMethod(Write.Method.STREAMING_INSERTS);
    options.setReadMethod(TypedRead.Method.DEFAULT);

    String streamingDestination =
        String.format("%s:%s.%s", project, DATASET_ID, STREAMING_TEST_TABLE);
    options.setOutput(streamingDestination);
    options.setInputTable(streamingDestination);

    runTestWriteRead(options);
  }

  @BeforeClass
  public static void setupTestEnvironment() {
    PipelineOptionsFactory.register(BigQueryIOJsonOptions.class);
  }

  public interface BigQueryIOJsonOptions extends TestPipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    String getInputTable();

    void setInputTable(String value);

    @Description("Query used to read from BigQuery")
    @Default.String("")
    String getInputQuery();

    void setInputQuery(String query);

    @Description("Read method used to read from BigQuery")
    @Default.Enum("EXPORT")
    TypedRead.Method getReadMethod();

    void setReadMethod(TypedRead.Method value);

    @Description(
        "BigQuery table to write to, specified as "
            + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    @Description("Write method used to write to BigQuery")
    @Default.Enum("STORAGE_WRITE_API")
    BigQueryIO.Write.Method getWriteMethod();

    void setWriteMethod(BigQueryIO.Write.Method value);
  }

  private static Map<String, Map<String, Object>> generateCountryData() {
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

    JSONObject usaCities = new JSONObject();
    usaCities.put("nyc", nyc);
    usaCities.put("la", la);
    usaCities.put("chicago", chicago);

    JSONArray usaLeaders = new JSONArray();
    usaLeaders.put("Donald Trump");
    usaLeaders.put("Barack Obama");
    usaLeaders.put("George W. Bush");
    usaLeaders.put("Bill Clinton");

    usa.put("name", "United States of America");
    usa.put("population", 329484123);
    usa.put("cities", usaCities);
    usa.put("past_leaders", usaLeaders);
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
    brisbane.put("name", "Brisbane");
    brisbane.put("state", "Queensland");
    brisbane.put("population", 2560720);

    JSONObject ausCities = new JSONObject();
    ausCities.put("sydney", sydney);
    ausCities.put("melbourne", melbourne);
    ausCities.put("brisbane", brisbane);

    JSONArray ausLeaders = new JSONArray();
    ausLeaders.put("Malcolm Turnbull");
    ausLeaders.put("Tony Abbot");
    ausLeaders.put("Kevin Rudd");

    aus.put("name", "Australia");
    aus.put("population", 25687041);
    aus.put("cities", ausCities);
    aus.put("past_leaders", ausLeaders);
    aus.put("in_northern_hemisphere", false);

    JSONObject special = new JSONObject();

    JSONObject specialCities = new JSONObject();

    JSONObject basingse = new JSONObject();
    basingse.put("name", "Ba Sing Se");
    basingse.put("state", "The Earth Kingdom");
    basingse.put("population", 200000);

    JSONObject bikinibottom = new JSONObject();
    bikinibottom.put("name", "Bikini Bottom");
    bikinibottom.put("state", "The Pacific Ocean");
    bikinibottom.put("population", 50000);

    specialCities.put("basingse", basingse);
    specialCities.put("bikinibottom", bikinibottom);

    JSONArray specialArr = new JSONArray();

    specialArr.put("1");
    specialArr.put("2");
    specialArr.put("!@#$%^&*()_+");

    special.put(
        "name",
        "newline\n, form\f, tab\t, \"quotes\", \\backslash\\, backspace\b, \u0000_hex_\u0f0f");
    special.put("population", -123456789);
    special.put("cities", specialCities);
    special.put("past_leaders", specialArr);
    special.put("in_northern_hemisphere", true);

    // usa landmarks
    JSONObject statueOfLiberty = new JSONObject();
    statueOfLiberty.put("name", "Statue of Liberty");
    statueOfLiberty.put("cool rating", JSONObject.NULL);
    JSONObject goldenGateBridge = new JSONObject();
    goldenGateBridge.put("name", "Golden Gate Bridge");
    goldenGateBridge.put("cool rating", "very cool");
    JSONObject grandCanyon = new JSONObject();
    grandCanyon.put("name", "Grand Canyon");
    grandCanyon.put("cool rating", "very very cool");

    // australia landmarks
    JSONObject operaHouse = new JSONObject();
    operaHouse.put("name", "Sydney Opera House");
    operaHouse.put("cool rating", "amazing");
    JSONObject greatBarrierReef = new JSONObject();
    greatBarrierReef.put("name", "Great Barrier Reef");
    greatBarrierReef.put("cool rating", JSONObject.NULL);

    // special landmarks
    JSONObject hogwarts = new JSONObject();
    hogwarts.put("name", "Hogwarts School of WitchCraft and Wizardry");
    hogwarts.put("cool rating", "magical");
    JSONObject willyWonka = new JSONObject();
    willyWonka.put("name", "Willy Wonka's Factory");
    willyWonka.put("cool rating", JSONObject.NULL);
    JSONObject rivendell = new JSONObject();
    rivendell.put("name", "Rivendell");
    rivendell.put("cool rating", "precious");

    // stats
    JSONObject usGdp = new JSONObject();
    usGdp.put("gdp_per_capita", 58559.675);
    usGdp.put("currency", "constant 2015 US$");
    JSONObject usCo2 = new JSONObject();
    usCo2.put("co2 emissions", 15.241);
    usCo2.put("measurement", "metric tons per capita");
    usCo2.put("year", 2018);

    JSONObject ausGdp = new JSONObject();
    ausGdp.put("gdp_per_capita", 58043.581);
    ausGdp.put("currency", "constant 2015 US$");
    JSONObject ausCo2 = new JSONObject();
    ausCo2.put("co2 emissions", 15.476);
    ausCo2.put("measurement", "metric tons per capita");
    ausCo2.put("year", 2018);

    JSONObject specialGdp = new JSONObject();
    specialGdp.put("gdp_per_capita", 421.70);
    specialGdp.put("currency", "constant 200 BC gold");
    JSONObject specialCo2 = new JSONObject();
    specialCo2.put("co2 emissions", -10.79);
    specialCo2.put("measurement", "metric tons per capita");
    specialCo2.put("year", 2018);

    Map<String, Map<String, Object>> data = new HashMap<>();
    // Nested maps for testing
    data.put(
        "countries",
        ImmutableMap.of(
            "usa", usa.toString(),
            "aus", aus.toString(),
            "special", special.toString()));
    data.put(
        "cities",
        new HashMap<>(
            ImmutableMap.<String, Object>builder()
                .put("usa_nyc", nyc.toString())
                .put("usa_la", la.toString())
                .put("usa_chicago", chicago.toString())
                .put("aus_sydney", sydney.toString())
                .put("aus_melbourne", melbourne.toString())
                .put("aus_brisbane", brisbane.toString())
                .put("special_basingse", basingse.toString())
                .put("special_bikinibottom", bikinibottom.toString())
                .build()));
    data.put(
        "landmarks",
        new HashMap<>(
            ImmutableMap.<String, Object>builder()
                .put("usa_0", statueOfLiberty.toString())
                .put("usa_1", goldenGateBridge.toString())
                .put("usa_2", grandCanyon.toString())
                .put("aus_0", operaHouse.toString())
                .put("aus_1", greatBarrierReef.toString())
                .put("special_0", hogwarts.toString())
                .put("special_1", willyWonka.toString())
                .put("special_2", rivendell.toString())
                .build()));
    data.put(
        "stats",
        new HashMap<>(
            ImmutableMap.<String, Object>builder()
                .put("usa_gdp_per_capita", usGdp.toString())
                .put("usa_co2_emissions", usCo2.toString())
                .put("aus_gdp_per_capita", ausGdp.toString())
                .put("aus_co2_emissions", ausCo2.toString())
                .put("special_gdp_per_capita", specialGdp.toString())
                .put("special_co2_emissions", specialCo2.toString())
                .build()));
    // Nested maps for writing to BigQuery
    data.put(
        "usa",
        ImmutableMap.of(
            "country", usa.toString(),
            "cities",
                Arrays.asList(
                    ImmutableMap.of("city_name", "nyc", "city", nyc.toString()),
                    ImmutableMap.of("city_name", "la", "city", la.toString()),
                    ImmutableMap.of("city_name", "chicago", "city", chicago.toString())),
            "landmarks",
                Arrays.asList(
                    statueOfLiberty.toString(),
                    goldenGateBridge.toString(),
                    grandCanyon.toString()),
            "stats",
                ImmutableMap.of(
                    "gdp_per_capita", usGdp.toString(),
                    "co2_emissions", usCo2.toString())));
    data.put(
        "aus",
        ImmutableMap.of(
            "country", aus.toString(),
            "cities",
                Arrays.asList(
                    ImmutableMap.of("city_name", "sydney", "city", sydney.toString()),
                    ImmutableMap.of("city_name", "melbourne", "city", melbourne.toString()),
                    ImmutableMap.of("city_name", "brisbane", "city", brisbane.toString())),
            "landmarks", Arrays.asList(operaHouse.toString(), greatBarrierReef.toString()),
            "stats",
                ImmutableMap.of(
                    "gdp_per_capita", ausGdp.toString(),
                    "co2_emissions", ausCo2.toString())));
    data.put(
        "special",
        ImmutableMap.of(
            "country", special.toString(),
            "cities",
                Arrays.asList(
                    ImmutableMap.of("city_name", "basingse", "city", basingse.toString()),
                    ImmutableMap.of("city_name", "bikinibottom", "city", bikinibottom.toString())),
            "landmarks",
                Arrays.asList(hogwarts.toString(), willyWonka.toString(), rivendell.toString()),
            "stats",
                ImmutableMap.of(
                    "gdp_per_capita", specialGdp.toString(),
                    "co2_emissions", specialCo2.toString())));
    return data;
  }
}
