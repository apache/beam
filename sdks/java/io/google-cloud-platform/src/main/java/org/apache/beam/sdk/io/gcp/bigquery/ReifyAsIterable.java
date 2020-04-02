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

import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * This transforms turns a side input into a singleton PCollection that can be used as the main
 * input for another transform.
 */
public class ReifyAsIterable<T> extends PTransform<PCollection<T>, PCollection<Iterable<T>>> {
  @Override
  public PCollection<Iterable<T>> expand(PCollection<T> input) {
    final PCollectionView<Iterable<T>> view = input.apply(View.asIterable());
    return input
        .getPipeline()
        .apply(Create.of((Void) null).withCoder(VoidCoder.of()))
        .apply(
            ParDo.of(
                    new DoFn<Void, Iterable<T>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(c.sideInput(view));
                      }
                    })
                .withSideInputs(view))
        .setCoder(IterableCoder.of(input.getCoder()));
  }
}
