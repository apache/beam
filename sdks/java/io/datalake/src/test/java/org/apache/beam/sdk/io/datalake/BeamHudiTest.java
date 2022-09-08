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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.SparkConf;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.config.HoodieWriteConfig.TBL_NAME;

public class BeamHudiTest {

    private String HUDI_PATH =       "/tmp/hudi/user";
    private String HUDI =    "hudi";
    private String OUTPUT_PATH = "/tmp/output/fromhudi/user.txt";

    @Test
    public void testWriteDataToHudi(){
        Map<String, String> options = new HashMap<String, String>();
        options.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "user_name");
        options.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "user_name");
        options.put(TBL_NAME.key(), "user");

        User user = new User("Tom", 28);
        BeamDataLakeUtil.writeToDataLake(user, HUDI, HUDI_PATH, getHudiSparkConf(), options);
    }

    @Test
    public void testReadDataFromHudi(){
        BeamDataLakeUtil.readFromDataLake( HUDI, HUDI_PATH, getHudiSparkConf(), OUTPUT_PATH);
    }

    private SparkConf getHudiSparkConf(){
        SparkConf sparkConf = new SparkConf().setAppName("hudi").setMaster("local")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        return sparkConf;
    }
}
