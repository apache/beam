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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RouteFilesTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private PCollectionTuple route(String pathPattern, String table, String... paths) {
    PCollectionTuple out =
        pipeline
            .apply(Create.of(Arrays.asList(paths)))
            .apply(
                ParDo.of(new RouteFiles(pathPattern, table))
                    .withOutputTags(RouteFiles.ROUTED, TupleTagList.of(RouteFiles.ERRORS)));
    out.get(RouteFiles.ERRORS).setRowSchema(RouteFiles.ERROR_SCHEMA);
    return out;
  }

  @Test
  public void routesEachFileToTheTableItsFoldersName() {
    PCollectionTuple out =
        route(
            "gs://lake/{db}/{tbl}/**",
            "warehouse.{db}.{tbl}",
            "gs://lake/sales/orders/part-0.parquet",
            "gs://lake/sales/orders/part-1.parquet",
            "gs://lake/eu/customers/part-0.parquet");

    PAssert.that(out.get(RouteFiles.ROUTED))
        .containsInAnyOrder(
            KV.of("warehouse.sales.orders", "gs://lake/sales/orders/part-0.parquet"),
            KV.of("warehouse.sales.orders", "gs://lake/sales/orders/part-1.parquet"),
            KV.of("warehouse.eu.customers", "gs://lake/eu/customers/part-0.parquet"));
    PAssert.that(out.get(RouteFiles.ERRORS)).empty();

    pipeline.run().waitUntilFinish();
  }

  /** A mistyped pattern has to be visible, so an unmatched path is a row and not a drop. */
  @Test
  public void aPathThatDoesNotMatchBecomesAnErrorRow() {
    PCollectionTuple out =
        route(
            "gs://lake/{db}/{tbl}/**",
            "warehouse.{db}.{tbl}",
            "gs://lake/sales/orders/part-0.parquet",
            "gs://lake/stray.parquet");

    PAssert.that(out.get(RouteFiles.ROUTED))
        .containsInAnyOrder(
            KV.of("warehouse.sales.orders", "gs://lake/sales/orders/part-0.parquet"));
    PAssert.that(out.get(RouteFiles.ERRORS))
        .containsInAnyOrder(
            Row.withSchema(RouteFiles.ERROR_SCHEMA)
                .addValues(
                    "gs://lake/stray.parquet",
                    "did not match path_pattern 'gs://lake/{db}/{tbl}/**'",
                    "")
                .build());

    pipeline.run().waitUntilFinish();
  }

  /** A template using none of the captures is legal: every folder merges into one table. */
  @Test
  public void aTemplateWithoutPlaceholdersSendsEverythingToOneTable() {
    PCollectionTuple out =
        route(
            "gs://lake/{tbl}/**", "lake.events",
            "gs://lake/a/f.parquet", "gs://lake/b/f.parquet");

    PAssert.that(out.get(RouteFiles.ROUTED))
        .containsInAnyOrder(
            KV.of("lake.events", "gs://lake/a/f.parquet"),
            KV.of("lake.events", "gs://lake/b/f.parquet"));

    pipeline.run().waitUntilFinish();
  }

  /** Config errors belong at construction, not once per file in a worker. */
  @Test
  public void aMalformedPatternFailsAtConstruction() {
    assertThrows(
        IllegalArgumentException.class, () -> new RouteFiles("gs://lake/{tbl}/", "lake.{tbl}"));
  }

  @Test
  public void aTemplateNamingAnUncapturedPlaceholderFailsAtConstruction() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RouteFiles("gs://lake/{db}/{tbl}/**", "warehouse.{database}"));
  }
}
