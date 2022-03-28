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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class BigQueryIOJSONIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOJSONIT.class);

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

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
