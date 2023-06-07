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
package org.apache.beam.sdk.io.components.deadletterqueue.sinks;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ThrowingSink<T> extends PTransform<@NonNull PCollection<T>, @NonNull PDone> {

  @Override
  public PDone expand(@NonNull PCollection<T> input) {
    input.apply(ParDo.of(new ThrowingDoFn()));

    return PDone.in(input.getPipeline());
  }

  public class ThrowingDoFn extends DoFn<T, Null> {

    @ProcessElement
    public void processElement(@Element @NonNull T element){
      throw new RuntimeException(element.toString());
    }
  }
}
