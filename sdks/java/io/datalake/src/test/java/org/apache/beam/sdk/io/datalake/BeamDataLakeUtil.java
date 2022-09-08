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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class BeamDataLakeUtil {

    /**
     * write to datalake
     * @param format delta, iceberg, hudi
     * @param path data storage path
     * @param sparkConf
     * @param dataLakeOptions
     */
    public static void writeToDataLake(User user,
                                       String format,
                                       String path,
                                       SparkConf sparkConf,
                                       Map<String, String> dataLakeOptions){
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<User> pcMid = pipeline.apply(Create.of(user));

        //transform to Row
        PCollection<Row> pcMid2 = pcMid.apply(ParDo.of(new UserToRow()));

        DataLakeIO.Write write = DataLakeIO.write()
                .withFormat(format)
                .withMode("append")
                .withSchema(getDataLakeSchema())
                .withPath(path)
                .withSparkConf(sparkConf)
                .withOptions(dataLakeOptions);
        if(dataLakeOptions != null){
            write.withOptions(dataLakeOptions);
        }

        pcMid2.apply(write);

        pipeline.run().waitUntilFinish();
    }

    /**
     * read from datalake
     * @param format delta, iceberg, hudi
     * @param path data storage path
     * @param sparkConf
     */
    public static void readFromDataLake(String format, String path, SparkConf sparkConf, String outPutPath){
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<User> pcStart = pipeline.apply(DataLakeIO.<User>read()
                .withFormat(format)
                .withPath(path)
                .withSparkConf(sparkConf)
                .withCoder(SerializableCoder.of(User.class))
                .withRowMapper(getRowMapper())
        );

        // transform
        PCollection<String> pcMid = pcStart.apply(ParDo.of(new UserToString()));

        // output to text
        pcMid.apply(TextIO.write().to(outPutPath));

        pipeline.run().waitUntilFinish();

    }

    /**
     * get Schema
     * @return
     */
    private static StructType getDataLakeSchema(){
        return new StructType(new StructField[] {
                new StructField("user_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("user_age", DataTypes.IntegerType, true, Metadata.empty())
        });
    }

    static class UserToRow extends DoFn<User, Row> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            User input = context.element();

            org.apache.spark.sql.Row row = RowFactory.create(
                    input.getUser_name(),
                    input.getUser_age()
            );
            context.output(row);
        }
    }

    static class UserToString extends DoFn<User, String> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            User input = context.element();

            context.output(input.toString());
        }
    }

    private static DataLakeIO.RowMapper<User> getRowMapper(){
        DataLakeIO.RowMapper<User> rowMapper =
                rs -> new User(
                        (String)rs.getAs("user_name"),
                        (Integer)rs.getAs("user_age")
                );
        return rowMapper;
    }

}
