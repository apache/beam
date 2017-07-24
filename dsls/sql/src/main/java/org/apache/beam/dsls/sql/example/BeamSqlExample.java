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
package org.apache.beam.dsls.sql.example;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.sd.BeamRowCoder;
import org.apache.beam.sdk.sd.BeamRowType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example with
 * <pre>
 * mvn -pl dsls/sql compile exec:java \
 *  -Dexec.mainClass=org.apache.beam.dsls.sql.example.BeamSqlExample \
 *   -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 * </pre>
 *
 */
class BeamSqlExample {
  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    //define the input row format
    List<String> fieldNames = Arrays.asList("c1", "c2", "c3");
    List<Integer> fieldTypes = Arrays.asList(Types.INTEGER, Types.VARCHAR, Types.DOUBLE);
    BeamRowType type = BeamRowType.create(fieldNames, fieldTypes);
    BeamRow row = new BeamRow(type);
    row.addField(0, 1);
    row.addField(1, "row");
    row.addField(2, 1.0);

    //create a source PCollection with Create.of();
    PCollection<BeamRow> inputTable = PBegin.in(p).apply(Create.of(row)
        .withCoder(new BeamRowCoder(type)));

    //Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<BeamRow> outputStream = inputTable.apply(
        BeamSql.simpleQuery("select c1, c2, c3 from PCOLLECTION where c1=1"));

    //print the output record of case 1;
    outputStream.apply("log_result",
        MapElements.<BeamRow, Void>via(new SimpleFunction<BeamRow, Void>() {
      public Void apply(BeamRow input) {
        System.out.println("PCOLLECTION: " + input);
        return null;
      }
    }));

    //Case 2. run the query with BeamSql.query over result PCollection of case 1.
    PCollection<BeamRow> outputStream2 =
        PCollectionTuple.of(new TupleTag<BeamRow>("CASE1_RESULT"), outputStream)
        .apply(BeamSql.query("select c2, c3 from CASE1_RESULT where c1=1"));

    //print the output record of case 2;
    outputStream2.apply("log_result",
        MapElements.<BeamRow, Void>via(new SimpleFunction<BeamRow, Void>() {
      @Override
      public Void apply(BeamRow input) {
        System.out.println("TABLE_B: " + input);
        return null;
      }
    }));

    p.run().waitUntilFinish();
  }
}
