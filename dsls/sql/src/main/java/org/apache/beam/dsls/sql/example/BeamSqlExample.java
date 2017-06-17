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
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 */
class BeamSqlExample {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlExample.class);

  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    //define the input row format
    List<String> fieldNames = Arrays.asList("c1", "c2", "c3");
    List<Integer> fieldTypes = Arrays.asList(Types.INTEGER, Types.VARCHAR, Types.DOUBLE);
    BeamSqlRecordType type = BeamSqlRecordType.create(fieldNames, fieldTypes);
    BeamSqlRow row = new BeamSqlRow(type);
    row.addField(0, 1);
    row.addField(1, "row");
    row.addField(2, 1.0);

    //create a source PCollection with Create.of();
    PCollection<BeamSqlRow> inputTable = PBegin.in(p).apply(Create.of(row)
        .withCoder(new BeamSqlRowCoder(type)));

    //Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<BeamSqlRow> outputStream = inputTable.apply(
        BeamSql.simpleQuery("select c2, c3 from PCOLLECTION where c1=1"));

    //log out the output record;
    outputStream.apply("log_result",
        MapElements.<BeamSqlRow, Void>via(new SimpleFunction<BeamSqlRow, Void>() {
      public Void apply(BeamSqlRow input) {
        System.out.println("PCOLLECTION: " + input);
        return null;
      }
    }));

    //Case 2. run the query with BeamSql.query
    PCollection<BeamSqlRow> outputStream2 =
        PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_B"), inputTable)
        .apply(BeamSql.query("select c2, c3 from TABLE_B where c1=1"));

    //log out the output record;
    outputStream2.apply("log_result",
        MapElements.<BeamSqlRow, Void>via(new SimpleFunction<BeamSqlRow, Void>() {
      @Override
      public Void apply(BeamSqlRow input) {
        System.out.println("TABLE_B: " + input);
        return null;
      }
    }));

    p.run().waitUntilFinish();
  }
}
