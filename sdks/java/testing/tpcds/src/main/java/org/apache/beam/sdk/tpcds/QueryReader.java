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

import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;

/**
 * The QueryReader reads query file (the file's extension is '.sql' and content doesn't end with a
 * ';'), write the query as a string and return it.
 */
public class QueryReader {
  /**
   * Reads a query file (.sql), return the query as a string.
   *
   * @param queryFileName The name of the query file (such as "query1, query5...") which is stored
   *     in resource/queries directory
   * @return The query string stored in this file.
   * @throws Exception
   */
  public static String readQuery(String queryFileName) throws Exception {
    String path = "queries/" + queryFileName + ".sql";
    return Resources.toString(Resources.getResource(path), StandardCharsets.UTF_8);
  }

  /**
   * Parse query and get all its identifiers.
   *
   * @param queryString
   * @return Set of SQL query identifiers as strings.
   * @throws SqlParseException
   */
  public static Set<String> getQueryIdentifiers(String queryString) throws SqlParseException {
    SqlParser parser = SqlParser.create(queryString);
    SqlNode parsedQuery = parser.parseQuery();
    SqlTransformRunner.SqlIdentifierVisitor sqlVisitor =
        new SqlTransformRunner.SqlIdentifierVisitor();
    parsedQuery.accept(sqlVisitor);
    return sqlVisitor.getIdentifiers();
  }
}
