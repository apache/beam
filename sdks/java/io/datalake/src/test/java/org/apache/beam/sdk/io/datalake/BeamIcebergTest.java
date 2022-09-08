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
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class BeamIcebergTest {

    private String ICEBERG_ROOT_PATH =      "/tmp/iceberg/user";
    private String ICEBERG_PATH = "local.db20220803_001.user_2";
    private String ICEBERG = "iceberg";
    private String OUTPUT_PATH = "/tmp/output/fromiceberg/user.txt";

    @Test
    public void initIcebergTable() {
        SparkSession sparkSession = SparkSession.builder().config(getIcebergSparkConf()).getOrCreate();
        sparkSession.sql("CREATE TABLE " + ICEBERG_PATH + " (" +
                " user_name STRING," +
                " user_age INTEGER" +
                ")" +
                " USING iceberg");
        sparkSession.close();
    }

    @Test
    public void testWriteDataToIceberg(){
        User user = new User("Tom", 28);
        BeamDataLakeUtil.writeToDataLake( user, ICEBERG, ICEBERG_PATH, getIcebergSparkConf(), null);
    }

    @Test
    public void testReadDataFromIceberg(){
        BeamDataLakeUtil.readFromDataLake( ICEBERG, ICEBERG_PATH,  getIcebergSparkConf(), OUTPUT_PATH);
    }

    private SparkConf getIcebergSparkConf(){
        SparkConf sparkConf = new SparkConf().setAppName("iceberg").setMaster("local")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
                .set("spark.sql.catalog.spark_catalog.type","hive")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse", ICEBERG_ROOT_PATH);
        return sparkConf;
    }

}
