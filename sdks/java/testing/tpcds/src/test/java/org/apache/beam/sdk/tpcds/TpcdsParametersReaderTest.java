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

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TpcdsParametersReaderTest {
    private TpcdsOptions tpcdsOptions;
    private TpcdsOptions tpcdsOptionsError;

    @Before
    public void initializeTpcdsOptions() {
        tpcdsOptions = PipelineOptionsFactory.as(TpcdsOptions.class);
        tpcdsOptionsError = PipelineOptionsFactory.as(TpcdsOptions.class);

        tpcdsOptions.setDataSize("1G");
        tpcdsOptions.setQueries("1,2,3");
        tpcdsOptions.setTpcParallel(2);

        tpcdsOptionsError.setDataSize("5G");
        tpcdsOptionsError.setQueries("0,100");
        tpcdsOptionsError.setTpcParallel(0);
    }

    @Test
    public void testGetAndCheckDataSize() throws Exception {
        String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
        String expected = "1G";
        assertEquals(expected, dataSize);
    }

    @Test( expected = Exception.class)
    public void testGetAndCheckDataSizeException() throws Exception {
        TpcdsParametersReader.getAndCheckDataSize(tpcdsOptionsError);
    }

    @Test
    public void testGetAndCheckQueries() throws Exception {
        TpcdsOptions tpcdsOptionsAll = PipelineOptionsFactory.as(TpcdsOptions.class);
        tpcdsOptionsAll.setQueries("all");
        String[] queryNameArray = TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptionsAll);
        String[] expected = new String[99];
        for (int i = 0; i < 99; i++) {
            expected[i] = "query" + (i + 1);
        }
        Assert.assertArrayEquals(expected, queryNameArray);
    }

    @Test
    public void testGetAndCheckAllQueries() throws Exception {
        String[] queryNameArray = TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptions);
        String[] expected = {"query1", "query2", "query3"};
        Assert.assertArrayEquals(expected, queryNameArray);
    }

    @Test( expected = Exception.class)
    public void testGetAndCheckQueriesException() throws Exception {
        TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptionsError);
    }

    @Test
    public void testGetAndCheckTpcParallel() throws Exception {
        int nThreads = TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptions);
        int expected = 2;
        assertEquals(expected, nThreads);
    }

    @Test( expected = Exception.class)
    public void ttestGetAndCheckTpcParallelException() throws Exception {
        TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptionsError);
    }
}
