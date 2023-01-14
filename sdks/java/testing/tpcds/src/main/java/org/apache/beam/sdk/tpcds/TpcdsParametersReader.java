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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Get and check the TpcdsOptions' parameters, throw exceptions when user input is invalid. */
public class TpcdsParametersReader {

  /** The data sizes that have been supported. */
  private static final Set<String> supportedDataSizes =
      Stream.of("1G", "1GB", "10G", "10GB", "100G", "100GB")
          .collect(Collectors.toCollection(HashSet::new));

  private static final String QUERY_PREFIX = "query";

  public static final List<String> ALL_QUERY_NAMES = getAllQueryNames();

  /**
   * Get and check dataSize entered by user. This dataSize has to have been supported.
   *
   * @param tpcdsOptions TpcdsOptions object constructed from user input
   * @return The dateSize user entered, if it is contained in supportedDataSizes set.
   * @throws Exception
   */
  public static String getAndCheckDataSize(TpcdsOptions tpcdsOptions) throws Exception {
    String dataSize = tpcdsOptions.getDataSize();

    if (!supportedDataSizes.contains(dataSize)) {
      throw new Exception("The data size you entered has not been supported.");
    }

    return dataSize;
  }

  /**
   * Get and check queries entered by user. This has to be a string of numbers separated by commas
   * or "all" which means run all 99 queries. All query numbers have to be between 1 and 99. Some
   * queries (14, 23, 24 and 39) may contain suffixes "a" or "b", e.g. "14a" and "14b".
   *
   * @param tpcdsOptions TpcdsOptions object constructed from user input
   * @return An array of query names, for example "1,2,14b" will be output as
   *     "query1,query2,query14b"
   * @throws Exception
   */
  public static String[] getAndCheckQueryNames(TpcdsOptions tpcdsOptions) {
    if (tpcdsOptions.getQueries().equalsIgnoreCase("all")) {
      // All TPC-DS queries need to be executed.
      return ALL_QUERY_NAMES.toArray(new String[0]);
    } else {
      List<String> queries = new ArrayList<>();
      Arrays.stream(tpcdsOptions.getQueries().split("[\\s,]+", -1))
          .map(s -> QUERY_PREFIX + s)
          .forEach(
              query -> {
                if (!ALL_QUERY_NAMES.contains(query)) {
                  throw new IllegalArgumentException(
                      "The query \"" + query + "\" is not supported.");
                }
                queries.add(query);
              });
      return queries.toArray(new String[0]);
    }
  }

  private static List<String> getAllQueryNames() {
    List<String> queries = new ArrayList<>();

    for (int i = 1; i <= 99; i++) {
      switch (i) {
        case 14:
        case 23:
        case 24:
        case 39:
          queries.add(QUERY_PREFIX + i + "a");
          queries.add(QUERY_PREFIX + i + "b");
          break;
        default:
          queries.add(QUERY_PREFIX + i);
          break;
      }
    }
    return queries;
  }

  /**
   * Get and check TpcParallel entered by user. This has to be an integer between 1 and 99.
   *
   * @param tpcdsOptions TpcdsOptions object constructed from user input.
   * @return The TpcParallel user entered.
   * @throws Exception
   */
  public static int getAndCheckTpcParallel(TpcdsOptions tpcdsOptions) throws Exception {
    int nThreads = tpcdsOptions.getTpcParallel();

    if (nThreads < 1 || nThreads > 99) {
      throw new Exception(
          "The TpcParallel your entered is invalid, please provide an integer between 1 and 99.");
    }

    return nThreads;
  }
}
