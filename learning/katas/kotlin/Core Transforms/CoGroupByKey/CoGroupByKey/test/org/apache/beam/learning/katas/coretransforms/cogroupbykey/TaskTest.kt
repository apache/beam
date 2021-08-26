/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.beam.learning.katas.coretransforms.cogroupbykey

import org.apache.beam.learning.katas.coretransforms.cogroupbykey.Task.applyTransform
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.junit.Rule
import org.junit.Test

class TaskTest {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun core_transforms_cogroupbykey_cogroupbykey() {
    val fruits = testPipeline.apply(
      "Fruits",
      Create.of("apple", "banana", "cherry")
    )

    val countries = testPipeline.apply(
      "Countries",
      Create.of("australia", "brazil", "canada")
    )

    val results = applyTransform(fruits, countries)

    PAssert.that(results).containsInAnyOrder(
      "WordsAlphabet{alphabet='a', fruit='apple', country='australia'}",
      "WordsAlphabet{alphabet='b', fruit='banana', country='brazil'}",
      "WordsAlphabet{alphabet='c', fruit='cherry', country='canada'}"
    )

    testPipeline.run().waitUntilFinish()
  }
}