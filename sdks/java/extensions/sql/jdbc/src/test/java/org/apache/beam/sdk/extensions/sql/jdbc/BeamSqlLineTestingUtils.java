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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static java.util.stream.Collectors.toList;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Util functions for BeamSqlLine related tests. */
class BeamSqlLineTestingUtils {

  private static final String QUERY_ARG = "-e";

  public static String[] buildArgs(String... strs) {
    List<String> argsList = new ArrayList();
    for (String str : strs) {
      argsList.add(QUERY_ARG);
      argsList.add(str);
    }
    return argsList.toArray(new String[argsList.size()]);
  }

  public static List<List<String>> toLines(ByteArrayOutputStream outputStream) {
    List<String> outputLines = Arrays.asList(outputStream.toString().split("\n"));
    return outputLines.stream().map(BeamSqlLineTestingUtils::splitFields).collect(toList());
  }

  private static List<String> splitFields(String outputLine) {
    return Arrays.stream(outputLine.split("\\|"))
        .map(field -> field.trim())
        .filter(field -> field.length() != 0)
        .collect(toList());
  }
}
