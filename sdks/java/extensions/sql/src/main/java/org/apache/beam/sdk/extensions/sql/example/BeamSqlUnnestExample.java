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
import org.apache.beam.sdk.values.*;

import java.util.Arrays;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example from the Beam source root with
 *
 * <pre>
 *   ./gradlew :sdks:java:extensions:sql:runNestedRowInArrayExample
 * </pre>
 *
 * <p>The above command executes the example locally using direct runner. Running the pipeline in
 * other runners require additional setup and are out of scope of the SQL examples. Please consult
 * Beam documentation on how to run pipelines.
 */
class BeamSqlUnnestExample {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // define the input row format level3
    Schema level3Type =
            Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();

    Row level3Row1 = Row.withSchema(level3Type).addValues(1, "row", 1.0).build();
    Row level3Row2 = Row.withSchema(level3Type).addValues(2, "row", 2.0).build();
    Row level3Row3 = Row.withSchema(level3Type).addValues(3, "row", 3.0).build();

    // define the input row format level3
    Schema level2Type =
            Schema.builder().addInt32Field("b1")
                    .addStringField("b2")
                    .addRowField("b3", level3Type)
                    .addDoubleField("b4").build();


    Row level2Row1 = Row.withSchema(level2Type).addValues(1, "row", level3Row1, 1.0).build();
    Row level2Row2 = Row.withSchema(level2Type).addValues(2, "row", level3Row2, 2.0).build();
    Row level2Row3 = Row.withSchema(level2Type).addValues(3, "row", level3Row3, 3.0).build();

    // define the input row format level3
    Schema level1Type =
            Schema.builder().addInt32Field("a1")
                    .addStringField("a2")
                    .addDoubleField("a3")
                    .addArrayField("a4", Schema.FieldType.row(level2Type))
                    .build();
    Row level1Row1 = Row.withSchema(level1Type).addValues(1, "row", 1.0,
            Arrays.asList(level2Row1, level2Row2, level2Row3)).build();
    Row level1Row2 = Row.withSchema(level1Type).addValues(2, "row", 2.0,
            Arrays.asList(level2Row1, level2Row2, level2Row3)).build();
    Row level1Row3 = Row.withSchema(level1Type).addValues(3, "row", 3.0,
            Arrays.asList(level2Row1, level2Row2, level2Row3)).build();


    // create a source PCollection with Create.of();
    PCollection<Row> inputTable =
            PBegin.in(p).apply(Create.of(level1Row1, level1Row2, level1Row3).withRowSchema(level1Type));

    String sql = "select t.a1, t.a2, t.a3, d.b1, d.b2, d.b4, d.b3.c1, d.b3.c2, d.b3.c3 from test t cross join unnest(t.a4) d";
    // Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> dfTemp =
            PCollectionTuple.of(new TupleTag<>("test"), inputTable).apply(SqlTransform.query(sql));



    // print the output record of case 1;
    Schema dfTempSchema = dfTemp.getSchema();
    // with out the fix it will throw following exception
    // Caused by: java.lang.IllegalArgumentException: Row expected 10 fields. initialized with 8 fields.


    // with the changes in the Row.Java
    dfTemp
            .apply(
                    "log_result",
                    MapElements.via(
                            new SimpleFunction<Row, Row>() {
                              @Override
                              public Row apply(Row input) {
                                // expect output:
                                //  PCOLLECTION: [1, row, 1.0, 1, row, 1.0, 1, row, 1.0]
                                //  PCOLLECTION: [1, row, 1.0, 2, row, 2.0, 2, row, 2.0]
                                //  PCOLLECTION: [1, row, 1.0, 3, row, 3.0, 3, row, 3.0]
                                //  PCOLLECTION: [3, row, 3.0, 1, row, 1.0, 1, row, 1.0]
                                //  PCOLLECTION: [3, row, 3.0, 2, row, 2.0, 2, row, 2.0]
                                //  PCOLLECTION: [3, row, 3.0, 3, row, 3.0, 3, row, 3.0]
                                //  PCOLLECTION: [2, row, 2.0, 1, row, 1.0, 1, row, 1.0]
                                //  PCOLLECTION: [2, row, 2.0, 2, row, 2.0, 2, row, 2.0]
                                //  PCOLLECTION: [2, row, 2.0, 3, row, 3.0, 3, row, 3.0]

                                System.out.println("PCOLLECTION: " + input.getValues());
                                return input;
                              }
                            }))
            .setRowSchema(dfTempSchema);

    p.run().waitUntilFinish();
  }
}
