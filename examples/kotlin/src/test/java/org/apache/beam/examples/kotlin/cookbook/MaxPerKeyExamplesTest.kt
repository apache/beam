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
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Unit tests for [MaxPerKeyExamples].  */
@RunWith(JUnit4::class)
class MaxPerKeyExamplesTest {

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

    private val testRows: List<TableRow> = listOf(row1, row2, row3)
    private val kv1 = KV.of(6, 85.3)
    private val kv2 = KV.of(6, 45.3)
    private val kv3 = KV.of(7, 75.4)
    private val testKvs: List<KV<Int, Double>> = listOf(kv1, kv2, kv3)
    private val resultRow1 = TableRow().set("month", 6).set("max_mean_temp", 85.3)
    private val resultRow2 = TableRow().set("month", 6).set("max_mean_temp", 45.3)
    private val resultRow3 = TableRow().set("month", 7).set("max_mean_temp", 75.4)

    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    fun testExtractTempFn() {
        val results = pipeline.apply(Create.of(testRows)).apply(ParDo.of<TableRow, KV<Int, Double>>(MaxPerKeyExamples.ExtractTempFn()))
        PAssert.that(results).containsInAnyOrder(ImmutableList.of(kv1, kv2, kv3))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun testFormatMaxesFn() {
        val results = pipeline.apply(Create.of(testKvs)).apply(ParDo.of<KV<Int, Double>, TableRow>(MaxPerKeyExamples.FormatMaxesFn()))
        PAssert.that(results).containsInAnyOrder(resultRow1, resultRow2, resultRow3)
        pipeline.run().waitUntilFinish()
    }
}