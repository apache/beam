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
package org.apache.beam.sdk.io.gcp.bigquery;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CombineAsIterable<T> extends PTransform<PCollection<T>, PCollection<Iterable<T>>> {
  @Override
  public PCollection<Iterable<T>> expand(PCollection<T> input) {
    return input
        .apply(
            "assign single key",
            ParDo.of(
                new DoFn<T, KV<String, T>>() {
                  @ProcessElement
                  public void processElement(@Element T element, OutputReceiver<KV<String, T>> o) {
                    o.output(KV.of("key", element));
                  }
                }))
        .apply(GroupByKey.create())
        .apply(Values.create());
  }
}
