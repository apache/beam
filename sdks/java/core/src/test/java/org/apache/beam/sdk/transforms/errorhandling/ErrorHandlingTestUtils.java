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
package org.apache.beam.sdk.transforms.errorhandling;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

public class ErrorHandlingTestUtils {
  public static class ErrorSinkTransform
      extends PTransform<PCollection<BadRecord>, PCollection<Long>> {

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollection<Long> expand(
        PCollection<BadRecord> input) {
      if (input.isBounded() == IsBounded.BOUNDED) {
        return input.apply("Combine", Combine.globally(Count.<BadRecord>combineFn()));
      } else {
        return input
            .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
            .apply("Combine", Combine.globally(Count.<BadRecord>combineFn()).withoutDefaults());
      }
    }
  }

  public static class EchoErrorTransform
      extends PTransform<PCollection<BadRecord>, PCollection<BadRecord>> {

    @Override
    public PCollection<BadRecord> expand(PCollection<BadRecord> input) {
      return input;
    }
  }
}
