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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

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

  /**
   * Returns a new {@link BoundedReader} that reads from this source.
   */
  public abstract BoundedReader<T> createReader(PipelineOptions options) throws IOException;

  /**
   * A {@code Reader} that reads a bounded amount of input and supports some additional
   * operations, such as progress estimation and dynamic work rebalancing.
   *
   * <h3>Boundedness</h3>
   * <p>Once {@link #start} or {@link #advance} has returned false, neither will be called
   * again on this object.
   *
   * <h3>Thread safety</h3>
   * All methods will be run from the same thread except {@link #splitAtFraction}, which can be
   * called concurrently from a different thread (but there will not be multiple concurrent calls
   * to {@link #splitAtFraction} itself).
   * <p>If the source does not implement {@link #splitAtFraction}, you do not need to worry about
   * thread safety. If implemented, it must be safe to call {@link #splitAtFraction} concurrently
   * with other methods.
   *
   * <h3>Implementing {@link #splitAtFraction}</h3>
   * In the course of dynamic work rebalancing, the method {@link #splitAtFraction}
   * may be called concurrently with {@link #advance} or {@link #start}. It is critical that
   * their interaction is implemented in a thread-safe way, otherwise data loss is possible.
   *
   * <p>Sources which support dynamic work rebalancing should use
   * {@link com.google.cloud.dataflow.sdk.io.range.RangeTracker} to manage the (source-specific)
   * range of positions that is being split. If your source supports dynamic work rebalancing,
   * please use that class to implement it if possible; if not possible, please contact the team
   * at <i>dataflow-feedback@google.com</i>.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public abstract static class BoundedReader<T> extends Source.Reader<T> {
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
     * <p> By default, returns null to indicate that this cannot be estimated.
     *
     * @return A value in [0, 1] representing the fraction of this reader's current input
     *   read so far, or {@code null} if such an estimate is not available.
     */
    public Double getFractionConsumed() {
      return null;
    }

    @Override
    public abstract BoundedSource<T> getCurrentSource();

    /**
     * Tells the reader to narrow the range of the input it's going to read and give up
     * the remainder, so that the new range would contain approximately the given
     * fraction of the amount of data in the current range.
     * <p>Returns a {@code BoundedSource} representing the remainder.
     *
     * <h3>Detailed description</h3>
     * Assuming the following sequence of calls:
     * <pre>{@code
     *   BoundedSource<T> initial = reader.getCurrentSource();
     *   BoundedSource<T> residual = reader.splitAtFraction(fraction);
     *   BoundedSource<T> primary = reader.getCurrentSource();
     * }</pre>
     * <ul>
     *  <li> The "primary" and "residual" sources, when read, should together cover the same
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
     *
     * <h3>Statefulness</h3>
     * Since this method (if successful) affects the reader's source, in subsequent invocations
     * "fraction" should be interpreted relative to the new current source.
     *
     * <h3>Thread safety and blocking</h3>
     * This method will be called concurrently to other methods (however there will not be multiple
     * concurrent invocations of this method itself), and it is critical for it to be implemented
     * in a thread-safe way (otherwise data loss is possible).
     *
     * <p>It is also very important that this method always completes quickly, in particular,
     * it should not perform or wait on any blocking operations such as I/O, RPCs etc. Violating
     * this requirement may stall completion of the work item or even cause it to fail.
     *
     * <p>E.g. it is incorrect to make both this method and {@link #start}/{@link #advance}
     * {@code synchronized}, because those methods can perform blocking operations, and then
     * this method would have to wait for those calls to complete.
     *
     * <p>{@link com.google.cloud.dataflow.sdk.io.range.RangeTracker} makes it easy to implement
     * this method safely and correctly.
     *
     * <p> By default, returns null to indicate that splitting is not possible.
     */
    public BoundedSource<T> splitAtFraction(double fraction) {
      return null;
    }

    /**
     * By default, returns the minimum possible timestamp.
     */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }
}
