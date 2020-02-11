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
package org.apache.beam.sdk.transforms.splittabledofn;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** Definitions and convenience methods for reporting sizes for SplittableDoFns. */
@Experimental(Kind.SPLITTABLE_DO_FN)
public final class Sizes {
  /**
   * {@link RestrictionTracker}s which can provide a size should implement this interface.
   * Implementations that do not implement this interface will be assumed to have an equivalent
   * size.
   */
  public interface HasSize {
    /**
     * A representation for the amount of known work represented as a size. Size {@code double}
     * representations should preferably represent a linear space.
     *
     * <p>It is up to each restriction tracker to convert between their natural representation of
     * outstanding work and this representation. For example:
     *
     * <ul>
     *   <li>Block based file source (e.g. Avro): From the end of the current block, the remaining
     *       number of bytes to the end of the restriction.
     *   <li>Pull based queue based source (e.g. Pubsub): The local/global size available in number
     *       of messages or number of {@code message bytes} that have not been processed.
     *   <li>Key range based source (e.g. Shuffle, Bigtable, ...): Scale the start key to be one and
     *       end key to be zero and interpolate the position of the next splittable key as the size.
     *       If information about the probability density function or cumulative distribution
     *       function is available, size interpolation can be improved. Alternatively, if the number
     *       of encoded bytes for the keys and values is known for the key range, the number of
     *       remaining bytes can be used.
     * </ul>
     */
    double getSize();
  }
}
