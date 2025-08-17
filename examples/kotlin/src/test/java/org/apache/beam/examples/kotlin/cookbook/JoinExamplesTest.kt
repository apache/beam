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
import org.apache.beam.examples.kotlin.cookbook.JoinExamples.ExtractCountryInfoFn
import org.apache.beam.examples.kotlin.cookbook.JoinExamples.ExtractEventDataFn
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Unit tests for [JoinExamples].  */
@RunWith(JUnit4::class)
class JoinExamplesTest {

    companion object {
        private val row1 = TableRow()
                .set("ActionGeo_CountryCode", "VM")
                .set("SQLDATE", "20141212")
                .set("Actor1Name", "BANGKOK")
                .set("SOURCEURL", "http://cnn.com")
        private val row2 = TableRow()
                .set("ActionGeo_CountryCode", "VM")
                .set("SQLDATE", "20141212")
                .set("Actor1Name", "LAOS")
                .set("SOURCEURL", "http://www.chicagotribune.com")
        private val row3 = TableRow()
                .set("ActionGeo_CountryCode", "BE")
                .set("SQLDATE", "20141213")
                .set("Actor1Name", "AFGHANISTAN")
                .set("SOURCEURL", "http://cnn.com")
        private val EVENTS = arrayOf(row1, row2, row3)
        val EVENT_ARRAY: List<TableRow> = listOf(*EVENTS)
        private val PARSED_EVENTS = listOf(
                KV.of("VM", "Date: 20141212, Actor1: LAOS, url: http://www.chicagotribune.com"),
                KV.of("BE", "Date: 20141213, Actor1: AFGHANISTAN, url: http://cnn.com"),
                KV.of("VM", "Date: 20141212, Actor1: BANGKOK, url: http://cnn.com"))
        private val PARSED_COUNTRY_CODES = listOf(KV.of("BE", "Belgium"), KV.of("VM", "Vietnam"))
        private val cc1 = TableRow().set("FIPSCC", "VM").set("HumanName", "Vietnam")
        private val cc2 = TableRow().set("FIPSCC", "BE").set("HumanName", "Belgium")
        private val CCS = arrayOf(cc1, cc2)
        val CC_ARRAY = listOf(*CCS)
        val JOINED_EVENTS = arrayOf(
                "Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: LAOS, "
                        + "url: http://www.chicagotribune.com",
                "Country code: VM, Country name: Vietnam, Event info: Date: 20141212, Actor1: BANGKOK, "
                        + "url: http://cnn.com",
                "Country code: BE, Country name: Belgium, Event info: Date: 20141213, Actor1: AFGHANISTAN, "
                        + "url: http://cnn.com")
    }

    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    fun testExtractEventDataFn() {
        val output = pipeline.apply(Create.of(EVENT_ARRAY)).apply(ParDo.of(ExtractEventDataFn()))
        PAssert.that(output).containsInAnyOrder(PARSED_EVENTS)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun testExtractCountryInfoFn() {
        val output = pipeline.apply(Create.of(CC_ARRAY)).apply(ParDo.of(ExtractCountryInfoFn()))
        PAssert.that(output).containsInAnyOrder(PARSED_COUNTRY_CODES)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun testJoin() {
        val input1 = pipeline.apply("CreateEvent", Create.of(EVENT_ARRAY))
        val input2 = pipeline.apply("CreateCC", Create.of(CC_ARRAY))
        val output: PCollection<String> = JoinExamples.joinEvents(input1, input2)
        PAssert.that(output).containsInAnyOrder(*JOINED_EVENTS)
        pipeline.run().waitUntilFinish()
    }
}