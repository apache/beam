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
package org.apache.beam.learning.katas.triggers.earlytriggers

import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors
import org.joda.time.Duration

class GenerateEvent : PTransform<PBegin, PCollection<String>>() {
  companion object {
    fun everySecond(): GenerateEvent {
      return GenerateEvent()
    }
  }

  override fun expand(input: PBegin): PCollection<String> {
    return input
      .apply(GenerateSequence.from(1).withRate(1, Duration.standardSeconds(1)))
      .apply(MapElements.into(TypeDescriptors.strings()).via(SerializableFunction { "event" }))
  }
}