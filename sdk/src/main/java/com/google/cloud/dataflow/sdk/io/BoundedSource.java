/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link Source} that reads a finite amount of input and, because of that, supports
 * some additional operations.
 *
 * <p>The operations are:
 * <ul>
 * <li>Splitting into bundles of given size: {@link #splitIntoBundles};
 * <li>Size estimation: {@link #getEstimatedSizeBytes};
 * <li>Telling whether or not this source produces key/value pairs in sorted order:
 * {@link #producesSortedKeys};
 * <li>The reader ({@link BoundedReader}) supports progress estimation
 * ({@link BoundedReader#getFractionConsumed}) and dynamic splitting
 * ({@link BoundedReader#splitAtFraction}).
 * </ul>
 *
 * <p> To use this class for supporting your custom input type, derive your class
 * class from it, and override the abstract methods. For an example, see {@link DatastoreIO}.
 *
 * @param <T> Type of records read by the source.
 */
public abstract class BoundedSource<T> extends Source<T> {
  private static final long serialVersionUID = 0L;

  /**
   * Splits the source into bundles of approximately given size (in bytes).
   */
  public abstract List<? extends BoundedSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception;

  /**
   * An estimate of the total size (in bytes) of the data that would be read from this source.
   * This estimate is in terms of external storage size, before any decompression or other
   * processing done by the reader.
   */
  public abstract long getEstimatedSizeBytes(PipelineOptions options) throws Exception;

  /**
   * Whether this source is known to produce key/value pairs with the (encoded) keys in
   * lexicographically sorted order.
   */
  public abstract boolean producesSortedKeys(PipelineOptions options) throws Exception;

  @Override
  public abstract BoundedReader<T> createReader(
      PipelineOptions options, @Nullable ExecutionContext executionContext) throws IOException;

  /**
   * A {@code Reader} that reads a bounded amount of input and supports some additional
   * operations, such as progress estimation and dynamic work rebalancing.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public interface BoundedReader<T> extends Source.Reader<T> {
    /**
     * Returns a value in [0, 1] representing approximately what fraction of the source
     * ({@link #getCurrentSource}) this reader has read so far.
     *
     * <p>It is recommended that this method should satisfy the following properties:
     * <ul>
     *   <li>Should return 0 before the {@link #start} call.
     *   <li>Should return 1 after a {@link #start} or {@link #advance} call that returns false.
     *   <li>The returned values should be non-decreasing (though they don't have to be unique).
     * </ul>
     *
     * @return A value in [0, 1] representing the fraction of this reader's current input
     *   read so far, or {@code null} if such an estimate is not available.
     */
    Double getFractionConsumed();

    @Override
    BoundedSource<T> getCurrentSource();

    /**
     * Tells the reader to narrow the range of the input it's going to read and give up
     * the remainder, so that the new range would contain approximately the given
     * fraction of the amount of data in the current range.
     * Returns a {@code BoundedSource} representing the remainder.
     * <p>
     * More detailed description: Assuming the following sequence of calls:
     * <pre>{@code
     *   BoundedSource<T> initial = reader.getCurrentSource();
     *   BoundedSource<T> residual = reader.splitAtFraction(fraction);
     *   BoundedSource<T> primary = reader.getCurrentSource();
     * }</pre>
     * <ul>
     *  <li> The "primary" and "residual" sources, when read, would together cover the same
     *  set of records as "initial".
     *  <li> The current reader should continue to be in a valid state, and continuing to read
     *  from it should, together with the records it already read, yield the same records
     *  as would have been read by "primary".
     *  <li> The amount of data read by "primary" should ideally represent approximately
     *  the given fraction of the amount of data read by "initial".
     * </ul>
     * For example, a reader that reads a range of offsets <i>[A, B)</i> in a file might implement
     * this method by truncating the current range to <i>[A, A + fraction*(B-A))</i> and returning
     * a Source representing the range <i>[A + fraction*(B-A), B)</i>.
     * <p>
     * This method should return {@code null} if the split cannot be performed for this fraction
     * while satisfying the semantics above. E.g., a reader that reads a range of offsets
     * in a file should return {@code null} if it is already past the position in its range
     * corresponding to the given fraction. In this case, the method MUST have no effect
     * (the reader must behave as if the method hadn't been called at all).
     * <p>
     * Since this method (if successful) affects the reader's source, in subsequent invocations
     * "fraction" should be interpreted relative to the new current source.
     */
    BoundedSource<T> splitAtFraction(double fraction);
  }

  /**
   * A base class implementing some optional methods of {@link BoundedReader} in a default way:
   * <ul>
   *   <li>Progress estimation ({@link #getFractionConsumed}) is not supported.
   *   <li>Dynamic splitting ({@link #splitAtFraction}) is not supported.
   * </ul>
   * @param <T>
   */
  public abstract static class AbstractBoundedReader<T>
      extends AbstractReader<T> implements BoundedReader<T> {
    @Override
    public Double getFractionConsumed() {
      return null;
    }

    @Override
    public BoundedSource<T> splitAtFraction(double fraction) {
      return null;
    }
  }
}
