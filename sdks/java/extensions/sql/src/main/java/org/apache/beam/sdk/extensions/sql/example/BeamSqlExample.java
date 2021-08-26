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
package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example from the Beam source root with
 *
 * <pre>
 *   ./gradlew :sdks:java:extensions:sql:runBasicExample
 * </pre>
 *
 * <p>The above command executes the example locally using direct runner. Running the pipeline in
 * other runners require additional setup and are out of scope of the SQL examples. Please consult
 * Beam documentation on how to run pipelines.
 */
class BeamSqlExample {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // define the input row format
    Schema type =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();

    Row row1 = Row.withSchema(type).addValues(1, "row", 1.0).build();
    Row row2 = Row.withSchema(type).addValues(2, "row", 2.0).build();
    Row row3 = Row.withSchema(type).addValues(3, "row", 3.0).build();

    // create a source PCollection with Create.of();
    PCollection<Row> inputTable =
        PBegin.in(p).apply(Create.of(row1, row2, row3).withRowSchema(type));

    // Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> outputStream =
        inputTable.apply(SqlTransform.query("select c1, c2, c3 from PCOLLECTION where c1 > 1"));

    // print the output record of case 1;
    outputStream
        .apply(
            "log_result",
            MapElements.via(
                new SimpleFunction<Row, Row>() {
                  @Override
                  public Row apply(Row input) {
                    // expect output:
                    //  PCOLLECTION: [3, row, 3.0]
                    //  PCOLLECTION: [2, row, 2.0]
                    System.out.println("PCOLLECTION: " + input.getValues());
                    return input;
                  }
                }))
        .setRowSchema(type);

    // Case 2. run the query with SqlTransform.query over result PCollection of case 1.
    PCollection<Row> outputStream2 =
        PCollectionTuple.of(new TupleTag<>("CASE1_RESULT"), outputStream)
            .apply(SqlTransform.query("select c2, sum(c3) from CASE1_RESULT group by c2"));

    // print the output record of case 2;
    outputStream2
        .apply(
            "log_result",
            MapElements.via(
                new SimpleFunction<Row, Row>() {
                  @Override
                  public Row apply(Row input) {
                    // expect output:
                    //  CASE1_RESULT: [row, 5.0]
                    System.out.println("CASE1_RESULT: " + input.getValues());
                    return input;
                  }
                }))
        .setRowSchema(
            Schema.builder().addStringField("stringField").addDoubleField("doubleField").build());

    p.run().waitUntilFinish();
  }
}
