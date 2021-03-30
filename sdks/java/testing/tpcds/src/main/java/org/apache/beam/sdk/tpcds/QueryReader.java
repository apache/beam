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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;

/**
 * The QueryReader reads query file (the file's extension is '.sql' and content doesn't end with a
 * ';'), write the query as a string and return it.
 */
public class QueryReader {
  public static String readQuery(String queryFileName) throws Exception {
    String path = "queries/" + queryFileName + ".sql";
    String query = Resources.toString(Resources.getResource(path), Charsets.UTF_8);
    return query;
  }

  /**
   * Reads a query file (.sql), return the query as a string.
   *
   * @param queryFileName The name of the query file (such as "query1, query5...") which is stored
   *     in resource/queries directory
   * @return The query string stored in this file.
   * @throws Exception
   */
  public static String readQuery2(String queryFileName) throws Exception {

    // Prepare the file reader.
    ClassLoader classLoader = QueryReader.class.getClassLoader();
    if (classLoader == null) {
      throw new RuntimeException("Can't get classloader from QueryReader.");
    }
    String path = "queries/" + queryFileName + ".sql";

    URL resource = classLoader.getResource(path);
    if (resource == null) {
      throw new RuntimeException("Resource for " + path + " can't be null.");
    }
    String queryFilePath = Objects.requireNonNull(resource).getPath();
    File queryFile = new File(queryFilePath);
    Reader fileReader =
        new InputStreamReader(new FileInputStream(queryFile), StandardCharsets.UTF_8);
    BufferedReader reader = new BufferedReader(fileReader);

    // Read the file into stringBuilder.
    StringBuilder stringBuilder = new StringBuilder();
    String line;
    String ls = System.getProperty("line.separator");
    while ((line = reader.readLine()) != null) {
      stringBuilder.append(line);
      stringBuilder.append(ls);
    }

    // Delete the last new line separator.
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    reader.close();

    return stringBuilder.toString();
  }
}
