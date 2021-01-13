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
package org.apache.beam.examples.kotlin.cookbook

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.examples.kotlin.cookbook.FilterExamples.FilterSingleMonthDataFn
import org.apache.beam.examples.kotlin.cookbook.FilterExamples.ProjectionFn
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Unit tests for [FilterExamples].  */
@RunWith(JUnit4::class)
class FilterExamplesTest {

    private val row1 = TableRow()
            .set("month", "6")
            .set("day", "21")
            .set("year", "2014")
            .set("mean_temp", "85.3")
            .set("tornado", true)
    private val row2 = TableRow()
            .set("month", "7")
            .set("day", "20")
            .set("year", "2014")
            .set("mean_temp", "75.4")
            .set("tornado", false)
    private val row3 = TableRow()
            .set("month", "6")
            .set("day", "18")
            .set("year", "2014")
            .set("mean_temp", "45.3")
            .set("tornado", true)

    private val outRow1 = TableRow().set("year", 2014).set("month", 6).set("day", 21).set("mean_temp", 85.3)
    private val outRow2 = TableRow().set("year", 2014).set("month", 7).set("day", 20).set("mean_temp", 75.4)
    private val outRow3 = TableRow().set("year", 2014).set("month", 6).set("day", 18).set("mean_temp", 45.3)

    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    @Category(ValidatesRunner::class)
    fun testProjectionFn() {
        val input = pipeline.apply(Create.of(row1, row2, row3))
        val results = input.apply(ParDo.of(ProjectionFn()))
        PAssert.that(results).containsInAnyOrder(outRow1, outRow2, outRow3)
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun testFilterSingleMonthDataFn() {
        val input = pipeline.apply(Create.of(outRow1, outRow2, outRow3))
        val results = input.apply(ParDo.of(FilterSingleMonthDataFn(7)))
        PAssert.that(results).containsInAnyOrder(outRow2)
        pipeline.run().waitUntilFinish()
    }
}