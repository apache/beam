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
package org.apache.beam.learning.katas.coretransforms.combine.combineperkey

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

object Task {
  const val PLAYER_1 = "Player 1"
  const val PLAYER_2 = "Player 2"
  const val PLAYER_3 = "Player 3"

  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val scores = pipeline.apply(
      Create.of(
        KV.of(PLAYER_1, 15), KV.of(PLAYER_2, 10), KV.of(PLAYER_1, 100),
        KV.of(PLAYER_3, 25), KV.of(PLAYER_2, 75)
      )
    )

    val output = applyTransform(scores)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(input: PCollection<KV<String, Int>>): PCollection<KV<String, Int>> {
    return input.apply<PCollection<KV<String, Int>>>(Combine.perKey(SumIntBinaryCombineFn()))
  }

  internal class SumIntBinaryCombineFn : BinaryCombineFn<Int>() {
    override fun apply(left: Int, right: Int): Int {
      return left + right
    }
  }
}