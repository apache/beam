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
package org.apache.beam.sdk.schemas.io;

import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Internal
public class DeadLetteredTransform<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  private final SimpleFunction<InputT, OutputT> transform;
  private final PTransform<PCollection<Failure>, PDone> deadLetter;

  public DeadLetteredTransform(SimpleFunction<InputT, OutputT> transform, String deadLetterConfig) {
    this(transform, GenericDlq.getDlqTransform(deadLetterConfig));
  }

  @VisibleForTesting
  DeadLetteredTransform(
      SimpleFunction<InputT, OutputT> transform,
      PTransform<PCollection<Failure>, PDone> deadLetter) {
    this.transform = transform;
    this.deadLetter = deadLetter;
  }

  // Required to capture the generic type parameter of the PCollection.
  private <RealInputT extends InputT> PCollection<OutputT> expandInternal(
      PCollection<RealInputT> input) {
    Coder<RealInputT> coder = input.getCoder();
    SerializableFunction<RealInputT, OutputT> localTransform = transform::apply;
    MapElements.MapWithFailures<RealInputT, OutputT, Failure> mapWithFailures =
        MapElements.into(transform.getOutputTypeDescriptor())
            .via(localTransform)
            .exceptionsInto(TypeDescriptor.of(Failure.class))
            .exceptionsVia(
                x -> {
                  try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                    coder.encode(x.element(), os);
                    return Failure.newBuilder()
                        .setPayload(os.toByteArray())
                        .setError(
                            String.format(
                                "%s%n%n%s",
                                x.exception().getMessage(),
                                ExceptionUtils.getStackTrace(x.exception())))
                        .build();
                  }
                });
    Result<PCollection<OutputT>, Failure> result = mapWithFailures.expand(input);
    result.failures().apply(deadLetter);
    return result.output();
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    return expandInternal(input);
  }
}
