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

package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A {@link PTransform} which produces a single empty byte array at the minimum timestamp in the
 * {@link GlobalWindow}.
 *
 * <p>Users should instead use {@link Create} or another {@link Read} transform to begin consuming
 * elements.
 */
@Internal
public class Impulse extends PTransform<PBegin, PCollection<byte[]>> {
  /**
   * Create a new {@link Impulse} {@link PTransform}.
   */
  // TODO: Make public and implement the default expansion of Read with Impulse -> ParDo
  static Impulse create() {
    return new Impulse();
  }

  private Impulse() {}

  @Override
  public PCollection<byte[]> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        WindowingStrategy.globalDefault(),
        IsBounded.BOUNDED,
        ByteArrayCoder.of());
  }
}
