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
public class BeamSqlExample {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlExample.class);

  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    //define the input row format
    BeamSqlRecordType type = new BeamSqlRecordType();
    type.addField("c1", Types.INTEGER);
    type.addField("c2", Types.VARCHAR);
    type.addField("c3", Types.DOUBLE);
    BeamSqlRow row = new BeamSqlRow(type);
    row.addField(0, 1);
    row.addField(1, "row");
    row.addField(2, 1.0);

    //create a source PCollection with Create.of();
    PCollection<BeamSqlRow> inputTable = PBegin.in(p).apply(Create.of(row)
        .withCoder(new BeamSqlRowCoder(type)));

    //Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<BeamSqlRow> outputStream = inputTable.apply(
        BeamSql.simpleQuery("select c2, c3 from TABLE_A where c1=1"));

    //log out the output record;
    outputStream.apply("log_result",
        MapElements.<BeamSqlRow, Void>via(new SimpleFunction<BeamSqlRow, Void>() {
      public Void apply(BeamSqlRow input) {
        System.out.println("TABLE_A: " + input);
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
