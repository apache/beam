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
package org.apache.beam.learning.katas.util

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.PCollection
import org.slf4j.LoggerFactory

object Log {
  private val LOG = LoggerFactory.getLogger(Log::class.java)
  fun <T> ofElements(): PTransform<PCollection<T>, PCollection<T>> {
    return LoggingTransform()
  }

  fun <T> ofElements(prefix: String): PTransform<PCollection<T>, PCollection<T>> {
    return LoggingTransform(prefix)
  }

  private class LoggingTransform<T>(private val prefix: String? = "") : PTransform<PCollection<T>, PCollection<T>>() {

    override fun expand(input: PCollection<T>): PCollection<T> {
      return input.apply(
        ParDo.of(
          object : DoFn<T, T>() {

            @ProcessElement
            fun processElement(@Element element: T, out: OutputReceiver<T>, window: BoundedWindow) {
              var message = prefix + element.toString()
              if (window !is GlobalWindow) {
                message = "$message  Window:$window"
              }

              LOG.info(message)

              out.output(element)
            }
          }
        )
      )
    }

  }

}