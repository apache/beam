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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options used to configure TPC-DS test */
public interface TpcdsOptions extends PipelineOptions {
    @Description("The size of TPC-DS data to run query on, user input should contain the unit, such as '1G', '10G'")
    String getDataSize();

    void setDataSize(String dataSize);

    // Set the return type to be String since reading from the command line (user input will be like "1,2,55" which represent TPC-DS query1, query3, query55)
    @Description("The queries numbers, read user input as string, numbers separated by commas")
    String getQueries();

    void setQueries(String queries);

    @Description("The number of queries to run in parallel")
    @Default.Integer(1)
    Integer getTpcParallel();

    void setTpcParallel(Integer parallelism);
}
