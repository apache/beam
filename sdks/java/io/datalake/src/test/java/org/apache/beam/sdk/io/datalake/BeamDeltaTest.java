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

package org.apache.beam.sdk.io.datalake;

import org.apache.spark.SparkConf;
import org.junit.Test;

public class BeamDeltaTest {
    private String DELTA_PATH =      "/tmp/delta/user";
    private String DELTA =   "delta";
    private String OUTPUT_PATH = "/tmp/output/fromdelta/user.txt";

    @Test
    public void testWriteDataToDelta(){
        User user = new User("Tom", 28);
        BeamDataLakeUtil.writeToDataLake(user, DELTA, DELTA_PATH, getDeltaSparkConf(), null);
    }

    @Test
    public void testReadDataFromDelta(){
        BeamDataLakeUtil.readFromDataLake( DELTA, DELTA_PATH, getDeltaSparkConf(), OUTPUT_PATH);
    }

    private SparkConf getDeltaSparkConf(){
        SparkConf sparkConf = new SparkConf().setAppName("delta").setMaster("local")
                .set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog");
        return sparkConf;
    }

}
