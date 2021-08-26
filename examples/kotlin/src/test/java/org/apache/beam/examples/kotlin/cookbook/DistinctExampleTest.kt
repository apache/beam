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

import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.Distinct
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Unit tests for [Distinct].  */
@RunWith(JUnit4::class)
class DistinctExampleTest {

    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    @Test
    @Category(ValidatesRunner::class)
    fun testDistinct() {
        val strings = listOf("k1", "k5", "k5", "k2", "k1", "k2", "k3")
        val input = pipeline.apply(Create.of(strings).withCoder(StringUtf8Coder.of()))
        val output = input.apply(Distinct.create())
        PAssert.that(output).containsInAnyOrder("k1", "k5", "k2", "k3")
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun testDistinctEmpty() {
        val strings = listOf<String>()
        val input = pipeline.apply(Create.of(strings).withCoder(StringUtf8Coder.of()))
        val output = input.apply(Distinct.create())
        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }
}