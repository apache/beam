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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Get and check the TpcdsOptions' parameters, throw exceptions when user input is invalid
 */
public class TpcdsParametersReader {
    /** The data sizes that have been supported. */
    private static final Set<String> supportedDataSizes = Stream.of("1G", "10G", "100G").collect(Collectors.toCollection(HashSet::new));

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
     * Get and check queries entered by user. This has to be a string of numbers separated by commas or "all" which means run all 99 queiries.
     * All query numbers have to be between 1 and 99.
     *
     * @param tpcdsOptions TpcdsOptions object constructed from user input
     * @return An array of query names, for example "1,2,7" will be output as "query1,query2,query7"
     * @throws Exception
     */
    public static String[] getAndCheckQueryNameArray(TpcdsOptions tpcdsOptions) throws Exception {
        String queryNums = tpcdsOptions.getQueries();

        String[] queryNumArr;
        if (queryNums.toLowerCase().equals("all")) {
            // All 99 TPC-DS queries need to be executed.
            queryNumArr = new String[99];
            for (int i = 0; i < 99; i++) {
                queryNumArr[i] = Integer.toString(i + 1);
            }
        } else {
            // Split user input queryNums by spaces and commas, get an array of all query numbers.
            queryNumArr = queryNums.split("[\\s,]+");

            for (String queryNumStr : queryNumArr) {
                try {
                    int queryNum = Integer.parseInt(queryNumStr);
                    if (queryNum < 1 || queryNum > 99) {
                        throw new Exception("The queries you entered contains invalid query number, please provide integers between 1 and 99.");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("The queries you entered should be integers, please provide integers between 1 and 99.");
                }
            }
        }

        String[] queryNameArr = new String[queryNumArr.length];
        for (int i = 0; i < queryNumArr.length; i++) {
            queryNameArr[i] = "query" + queryNumArr[i];
        }

        return queryNameArr;
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
            throw new Exception("The TpcParallel your entered is invalid, please provide an integer between 1 and 99.");
        }

        return nThreads;
    }
}
