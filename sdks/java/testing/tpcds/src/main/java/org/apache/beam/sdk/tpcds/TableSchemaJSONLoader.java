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
package org.apache.beam.sdk.tpcds;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.FileNameUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * TableSchemaJSONLoader can get all table's names from resource/schemas directory and parse a
 * table's schema into a string.
 */
public class TableSchemaJSONLoader {
  public static String readQuery(String tableName) throws Exception {
    String path = "schemas/" + tableName + ".json";
    String fixture = Resources.toString(Resources.getResource(path), Charsets.UTF_8);
    return fixture;
  }

  /**
   * Read a table schema json file from resource/schemas directory, parse the file into a string
   * which can be utilized by BeamSqlEnv.executeDdl method.
   *
   * @param tableName The name of the json file to be read (fo example: item, store_sales).
   * @return A string that matches the format in BeamSqlEnv.executeDdl method, such as "d_date_sk
   *     bigint, d_date_id varchar"
   * @throws Exception
   */
  @SuppressWarnings({"rawtypes", "DefaultCharset"})
  public static String parseTableSchema(String tableName) throws Exception {
    String path = "schemas/" + tableName + ".json";
    String schema = Resources.toString(Resources.getResource(path), Charsets.UTF_8);
    System.out.println("schema = " + schema);

    JSONObject jsonObject = (JSONObject) new JSONParser().parse(schema);
    JSONArray jsonArray = (JSONArray) jsonObject.get("schema");
    if (jsonArray == null) {
      throw new RuntimeException("Can't get Json array for \"schema\" key.");
    }

    // Iterate each element in jsonArray to construct the schema string
    StringBuilder schemaStringBuilder = new StringBuilder();

    Iterator jsonArrIterator = jsonArray.iterator();
    Iterator<Map.Entry> recordIterator;
    while (jsonArrIterator.hasNext()) {
      recordIterator = ((Map) jsonArrIterator.next()).entrySet().iterator();
      while (recordIterator.hasNext()) {
        Map.Entry pair = recordIterator.next();

        if (pair.getKey().equals("type")) {
          // If the key of the pair is "type", make some modification before appending it to the
          // schemaStringBuilder, then append a comma.
          String typeName = (String) pair.getValue();
          if (typeName.toLowerCase().equals("identifier")
              || typeName.toLowerCase().equals("integer")) {
            // Use long type to represent int, prevent overflow
            schemaStringBuilder.append("bigint");
          } else if (typeName.contains("decimal")) {
            // Currently Beam SQL doesn't handle "decimal" type properly, use "double" to replace it
            // for now.
            schemaStringBuilder.append("double");
          } else {
            // Currently Beam SQL doesn't handle "date" type properly, use "varchar" replace it for
            // now.
            schemaStringBuilder.append("varchar");
          }
          schemaStringBuilder.append(',');
        } else {
          // If the key of the pair is "name", directly append it to the StringBuilder, then append
          // a space.
          schemaStringBuilder.append((pair.getValue()));
          schemaStringBuilder.append(' ');
        }
      }
    }

    // Delete the last ',' in schema string
    if (schemaStringBuilder.length() > 0) {
      schemaStringBuilder.deleteCharAt(schemaStringBuilder.length() - 1);
    }

    String schemaString = schemaStringBuilder.toString();

    return schemaString;
  }

  /**
   * Get all tables' names. Tables are stored in resource/schemas directory in the form of json
   * files, such as "item.json", "store_sales.json", they'll be converted to "item", "store_sales".
   *
   * @return The list of names of all tables.
   */
  public static List<String> getAllTableNames() {
    ClassLoader classLoader = TableSchemaJSONLoader.class.getClassLoader();
    if (classLoader == null) {
      throw new RuntimeException("Can't get classloader from TableSchemaJSONLoader.");
    }
    URL resource = classLoader.getResource("schemas");
    if (resource == null) {
      throw new RuntimeException("Resource for \"schemas\" can't be null.");
    }
    String tableDirPath = Objects.requireNonNull(resource).getPath();
    File tableDir = new File(tableDirPath);
    File[] tableDirListing = tableDir.listFiles();

    List<String> tableNames = new ArrayList<>();

    if (tableDirListing != null) {
      for (File file : tableDirListing) {
        // Remove the .json extension in file name
        tableNames.add(FileNameUtils.getBaseName((file.getName())));
      }
    }

    return tableNames;
  }
}
