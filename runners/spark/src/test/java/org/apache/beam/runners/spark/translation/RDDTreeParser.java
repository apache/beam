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
package org.apache.beam.runners.spark.translation;

import java.util.ArrayList;
import java.util.List;

/** Utility class for parsing RDD Debug String. */
@SuppressWarnings("StringSplitter")
class RDDTreeParser {

  public static List<RDDNode> parse(String debugString) {
    List<RDDNode> list = new ArrayList<>();
    String[] lines = debugString.split("\n");

    for (String line : lines) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }

      int id = extractId(line);
      final String[] parsedString = line.replace("|", "").split(" at ");
      String name = parsedString[0].replaceAll("[+\\-]", "").replaceAll("\\(\\d+\\)", "").trim();
      String operation = parsedString[1].trim();
      String location = parsedString[2].trim();

      RDDNode node = new RDDNode(id, name, operation, location);

      list.add(node);
    }

    return list;
  }

  private static int extractId(String line) {
    String idPart = line.substring(line.indexOf('[') + 1, line.indexOf(']'));
    return Integer.parseInt(idPart);
  }
}
