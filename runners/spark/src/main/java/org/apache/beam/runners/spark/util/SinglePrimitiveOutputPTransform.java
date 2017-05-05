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
package org.apache.beam.runners.spark.util;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A {@link PTransform} wrapping another transform.
 */
public class SinglePrimitiveOutputPTransform<T> extends PTransform<PInput, PCollection<T>> {
  private PTransform<PInput, PCollection<T>> transform;

  public SinglePrimitiveOutputPTransform(PTransform<PInput, PCollection<T>> transform) {
    this.transform = transform;
  }

  @Override
  public PCollection<T> expand(PInput input) {
    try {
      PCollection<T> collection = PCollection.<T>createPrimitiveOutputInternal(
              input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
      collection.setCoder(transform.getDefaultOutputCoder(input, collection));
      return collection;
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(
          "Unable to infer a coder and no Coder was specified. "
              + "Please set a coder by invoking Create.withCoder() explicitly.",
          e);
    }
  }
}
