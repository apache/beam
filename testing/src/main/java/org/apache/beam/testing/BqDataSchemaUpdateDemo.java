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
package org.apache.beam.testing;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.services.bigquery.model.*;
import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class BqDataSchemaUpdateDemo {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    com.google.api.services.bigquery.model.TableSchema initialSchema =
        new com.google.api.services.bigquery.model.TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("col1").setType("INTEGER"),
                    new TableFieldSchema().setName("col2").setType("STRING")));

    BigQueryIO.Write<TableRow> writeTransform =
        BigQueryIO.writeTableRows()
            .to("apache-beam-testing.ahmedabualsaud.dynamic_schema_4")
            .withSchema(initialSchema)
            //            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API);

    WriteResult result =
        p.apply(GenerateSequence.from(0).to(100))
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(
                        i -> {
                          if (i < 50) {
                            return new TableRow().set("col1", i).set("col2", String.valueOf(i));
                          }
                          return new TableRow()
                              .set("col1", i)
                              .set("col2", String.valueOf(i))
                              .set("col3", i * 2)
                              .set("col4", i * 3);
                        }))
            .apply(writeTransform);

    PCollection<KV<String, TableRow>> rowsWithExtraCols =
        result.getFailedStorageApiInserts().apply(ParDo.of(new ExtractSchemaMismatchRows()));
    rowsWithExtraCols
        .apply(ParDo.of(UpdateSchemaDoFn.atInterval(Instant.ofEpochMilli(500))))
        .apply(writeTransform);

    p.run().waitUntilFinish();
  }

  public static class ExtractSchemaMismatchRows
      extends DoFn<BigQueryStorageApiInsertError, KV<String, TableRow>> {
    @ProcessElement
    public void process(
        @Element BigQueryStorageApiInsertError error, OutputReceiver<KV<String, TableRow>> out) {
      @Nullable String errorMessage = error.getErrorMessage();
      if (errorMessage != null && errorMessage.contains("TableRow contained unexpected field")) {
        System.out.println("error row: " + error);
        String tableSpec =
            checkStateNotNull(BigQueryUtils.toTableSpec(checkStateNotNull(error.getTable())));
        out.output(KV.of(tableSpec, error.getRow()));
      }
    }
  }
}
