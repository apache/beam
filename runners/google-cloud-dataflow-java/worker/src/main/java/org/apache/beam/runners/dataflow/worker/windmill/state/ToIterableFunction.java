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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;

/** Function to extract an {@link Iterable} from the continuation-supporting page read future. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ToIterableFunction<ContinuationT, ResultT>
    implements Function<ValuesAndContPosition<ResultT, ContinuationT>, Iterable<ResultT>> {
  private final StateTag<ContinuationT> stateTag;
  private final Coder<?> coder;
  /**
   * Reader to request continuation pages from, or {@literal null} if no continuation pages
   * required.
   */
  private @Nullable WindmillStateReader reader;

  public ToIterableFunction(
      WindmillStateReader reader, StateTag<ContinuationT> stateTag, Coder<?> coder) {
    this.reader = reader;
    this.stateTag = stateTag;
    this.coder = coder;
  }

  @SuppressFBWarnings(
      value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
      justification = "https://github.com/google/guava/issues/920")
  @Override
  public Iterable<ResultT> apply(
      @Nonnull ValuesAndContPosition<ResultT, ContinuationT> valuesAndContPosition) {
    if (valuesAndContPosition.getContinuationPosition() == null) {
      // Number of values is small enough Windmill sent us the entire bag in one response.
      reader = null;
      return valuesAndContPosition.getValues();
    } else {
      // Return an iterable which knows how to come back for more.
      StateTag.Builder<ContinuationT> continuationTBuilder =
          StateTag.of(
                  stateTag.getKind(),
                  stateTag.getTag(),
                  stateTag.getStateFamily(),
                  valuesAndContPosition.getContinuationPosition())
              .toBuilder();
      if (stateTag.getSortedListRange() != null) {
        continuationTBuilder.setSortedListRange(stateTag.getSortedListRange()).build();
      }
      if (stateTag.getMultimapKey() != null) {
        continuationTBuilder.setMultimapKey(stateTag.getMultimapKey()).build();
      }
      if (stateTag.getOmitValues() != null) {
        continuationTBuilder.setOmitValues(stateTag.getOmitValues()).build();
      }
      return new PagingIterable<>(
          reader, valuesAndContPosition.getValues(), continuationTBuilder.build(), coder);
    }
  }
}
