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
package org.apache.beam.runners.core;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.CausedByDrain;

/** Combiner for CombinedMetadata. */
class CombinedMetadataCombiner
    extends CombineFn<CombinedMetadata, CombinedMetadata, CombinedMetadata> {
  private static final CombinedMetadataCombiner INSTANCE = new CombinedMetadataCombiner();

  public static CombinedMetadataCombiner of() {
    return INSTANCE;
  }

  @Override
  public CombinedMetadata createAccumulator() {
    return CombinedMetadata.create(CausedByDrainCombiner.of().createAccumulator());
  }

  @Override
  public CombinedMetadata addInput(CombinedMetadata accumulator, CombinedMetadata input) {
    return CombinedMetadata.create(
        CausedByDrainCombiner.of().addInput(accumulator.causedByDrain(), input.causedByDrain()));
  }

  @Override
  public CombinedMetadata mergeAccumulators(Iterable<CombinedMetadata> accumulators) {
    CombinedMetadata result = createAccumulator();
    for (CombinedMetadata accum : accumulators) {
      result = addInput(result, accum);
    }
    return result;
  }

  @Override
  public CombinedMetadata extractOutput(CombinedMetadata accumulator) {
    return accumulator;
  }

  /** Combiner for CausedByDrain metadata. */
  static class CausedByDrainCombiner implements MetadataCombiner<CausedByDrain> {
    private static final CausedByDrainCombiner INSTANCE = new CausedByDrainCombiner();

    public static CausedByDrainCombiner of() {
      return INSTANCE;
    }

    @Override
    public CausedByDrain createAccumulator() {
      return CausedByDrain.NORMAL;
    }

    @Override
    public CausedByDrain addInput(CausedByDrain current, CausedByDrain input) {
      if (current == CausedByDrain.CAUSED_BY_DRAIN || input == CausedByDrain.CAUSED_BY_DRAIN) {
        return CausedByDrain.CAUSED_BY_DRAIN;
      }
      return CausedByDrain.NORMAL;
    }
  }
}
