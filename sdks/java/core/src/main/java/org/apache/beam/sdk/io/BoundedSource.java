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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.range.OffsetRangeTracker;
import org.apache.beam.sdk.io.range.RangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link Source} that reads a finite amount of input and, because of that, supports some
 * additional operations.
 *
 * <p>The operations are:
 *
 * <ul>
 *   <li>Splitting into sources that read bundles of given size: {@link #split};
 *   <li>Size estimation: {@link #getEstimatedSizeBytes};
 *   <li>The accompanying {@link BoundedReader reader} has additional functionality to enable
 *       runners to dynamically adapt based on runtime conditions.
 *       <ul>
 *         <li>Progress estimation ({@link BoundedReader#getFractionConsumed})
 *         <li>Tracking of parallelism, to determine whether the current source can be split ({@link
 *             BoundedReader#getSplitPointsConsumed()} and {@link
 *             BoundedReader#getSplitPointsRemaining()}).
 *         <li>Dynamic splitting of the current source ({@link BoundedReader#splitAtFraction}).
 *       </ul>
 * </ul>
 *
 * @param <T> Type of records read by the source.
 */
public abstract class BoundedSource<T> extends Source<T> {
  /** Splits the source into bundles of approximately {@code desiredBundleSizeBytes}. */
  public abstract List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception;

  /**
   * An estimate of the total size (in bytes) of the data that would be read from this source. This
   * estimate is in terms of external storage size, before any decompression or other processing
   * done by the reader.
   *
   * <p>If there is no way to estimate the size of the source implementations MAY return 0L.
   */
  public abstract long getEstimatedSizeBytes(PipelineOptions options) throws Exception;

  /** Returns a new {@link BoundedReader} that reads from this source. */
  public abstract BoundedReader<T> createReader(PipelineOptions options) throws IOException;

  /**
   * A {@code Reader} that reads a bounded amount of input and supports some additional operations,
   * such as progress estimation and dynamic work rebalancing.
   *
   * <h3>Boundedness</h3>
   *
   * <p>Once {@link #start} or {@link #advance} has returned false, neither will be called again on
   * this object.
   *
   * <h3>Thread safety</h3>
   *
   * <p>All methods will be run from the same thread except {@link #splitAtFraction}, {@link
   * #getFractionConsumed}, {@link #getCurrentSource}, {@link #getSplitPointsConsumed()}, and {@link
   * #getSplitPointsRemaining()}, all of which can be called concurrently from a different thread.
   * There will not be multiple concurrent calls to {@link #splitAtFraction}.
   *
   * <p>It must be safe to call {@link #splitAtFraction}, {@link #getFractionConsumed}, {@link
   * #getCurrentSource}, {@link #getSplitPointsConsumed()}, and {@link #getSplitPointsRemaining()}
   * concurrently with other methods.
   *
   * <p>Additionally, a successful {@link #splitAtFraction} call must, by definition, cause {@link
   * #getCurrentSource} to start returning a different value. Callers of {@link #getCurrentSource}
   * need to be aware of the possibility that the returned value can change at any time, and must
   * only access the properties of the source returned by {@link #getCurrentSource} which do not
   * change between {@link #splitAtFraction} calls.
   *
   * <h3>Implementing {@link #splitAtFraction}</h3>
   *
   * <p>In the course of dynamic work rebalancing, the method {@link #splitAtFraction} may be called
   * concurrently with {@link #advance} or {@link #start}. It is critical that their interaction is
   * implemented in a thread-safe way, otherwise data loss is possible.
   *
   * <p>Sources which support dynamic work rebalancing should use {@link
   * org.apache.beam.sdk.io.range.RangeTracker} to manage the (source-specific) range of positions
   * that is being split.
   */
  @Experimental(Kind.SOURCE_SINK)
  public abstract static class BoundedReader<T> extends Source.Reader<T> {
    /**
     * Returns a value in [0, 1] representing approximately what fraction of the {@link
     * #getCurrentSource current source} this reader has read so far, or {@code null} if such an
     * estimate is not available.
     *
     * <p>It is recommended that this method should satisfy the following properties:
     *
     * <ul>
     *   <li>Should return 0 before the {@link #start} call.
     *   <li>Should return 1 after a {@link #start} or {@link #advance} call that returns false.
     *   <li>The returned values should be non-decreasing (though they don't have to be unique).
     * </ul>
     *
     * <p>By default, returns null to indicate that this cannot be estimated.
     *
     * <h3>Thread safety</h3>
     *
     * If {@link #splitAtFraction} is implemented, this method can be called concurrently to other
     * methods (including itself), and it is therefore critical for it to be implemented in a
     * thread-safe way.
     */
    public @Nullable Double getFractionConsumed() {
      return null;
    }

    /**
     * A constant to use as the return value for {@link #getSplitPointsConsumed()} or {@link
     * #getSplitPointsRemaining()} when the exact value is unknown.
     */
    public static final long SPLIT_POINTS_UNKNOWN = -1;

    /**
     * Returns the total amount of parallelism in the consumed (returned and processed) range of
     * this reader's current {@link BoundedSource} (as would be returned by {@link
     * #getCurrentSource}). This corresponds to all split point records (see {@link RangeTracker})
     * returned by this reader, <em>excluding</em> the last split point returned if the reader is
     * not finished.
     *
     * <p>Consider the following examples: (1) An input that can be read in parallel down to the
     * individual records, such as {@link CountingSource#upTo}, is called "perfectly splittable".
     * (2) a "block-compressed" file format such as {@link AvroIO}, in which a block of records has
     * to be read as a whole, but different blocks can be read in parallel. (3) An "unsplittable"
     * input such as a cursor in a database.
     *
     * <ul>
     *   <li>Any {@link BoundedReader reader} that is unstarted (aka, has never had a call to {@link
     *       #start}) has a consumed parallelism of 0. This condition holds independent of whether
     *       the input is splittable.
     *   <li>Any {@link BoundedReader reader} that has only returned its first element (aka, has
     *       never had a call to {@link #advance}) has a consumed parallelism of 0: the first
     *       element is the current element and is still being processed. This condition holds
     *       independent of whether the input is splittable.
     *   <li>For an empty reader (in which the call to {@link #start} returned false), the consumed
     *       parallelism is 0. This condition holds independent of whether the input is splittable.
     *   <li>For a non-empty, finished reader (in which the call to {@link #start} returned true and
     *       a call to {@link #advance} has returned false), the value returned must be at least 1
     *       and should equal the total parallelism in the source.
     *   <li>For example (1): After returning record #30 (starting at 1) out of 50 in a perfectly
     *       splittable 50-record input, this value should be 29. When finished, the consumed
     *       parallelism should be 50.
     *   <li>For example (2): In a block-compressed value consisting of 5 blocks, the value should
     *       stay at 0 until the first record of the second block is returned; stay at 1 until the
     *       first record of the third block is returned, etc. Only once the end-of-file is reached
     *       then the fifth block has been consumed and the value should stay at 5.
     *   <li>For example (3): For any non-empty unsplittable input, the consumed parallelism is 0
     *       until the reader is finished (because the last call to {@link #advance} returned false,
     *       at which point it becomes 1.
     * </ul>
     *
     * <p>A reader that is implemented using a {@link RangeTracker} is encouraged to use the range
     * tracker's ability to count split points to implement this method. See {@link
     * OffsetBasedSource.OffsetBasedReader} and {@link OffsetRangeTracker} for an example.
     *
     * <p>Defaults to {@link #SPLIT_POINTS_UNKNOWN}. Any value less than 0 will be interpreted as
     * unknown.
     *
     * <h3>Thread safety</h3>
     *
     * See the javadoc on {@link BoundedReader} for information about thread safety.
     *
     * @see #getSplitPointsRemaining()
     */
    public long getSplitPointsConsumed() {
      return SPLIT_POINTS_UNKNOWN;
    }

    /**
     * Returns the total amount of parallelism in the unprocessed part of this reader's current
     * {@link BoundedSource} (as would be returned by {@link #getCurrentSource}). This corresponds
     * to all unprocessed split point records (see {@link RangeTracker}), including the last split
     * point returned, in the remainder part of the source.
     *
     * <p>This function should be implemented only <strong>in addition to {@link
     * #getSplitPointsConsumed()}</strong> and only if <em>an exact value can be returned</em>.
     *
     * <p>Consider the following examples: (1) An input that can be read in parallel down to the
     * individual records, such as {@link CountingSource#upTo}, is called "perfectly splittable".
     * (2) a "block-compressed" file format such as {@link AvroIO}, in which a block of records has
     * to be read as a whole, but different blocks can be read in parallel. (3) An "unsplittable"
     * input such as a cursor in a database.
     *
     * <p>Assume for examples (1) and (2) that the number of records or blocks remaining is known:
     *
     * <ul>
     *   <li>Any {@link BoundedReader reader} for which the last call to {@link #start} or {@link
     *       #advance} has returned true should should not return 0, because this reader itself
     *       represents parallelism at least 1. This condition holds independent of whether the
     *       input is splittable.
     *   <li>A finished reader (for which {@link #start} or {@link #advance}) has returned false
     *       should return a value of 0. This condition holds independent of whether the input is
     *       splittable.
     *   <li>For example 1: After returning record #30 (starting at 1) out of 50 in a perfectly
     *       splittable 50-record input, this value should be 21 (20 remaining + 1 current) if the
     *       total number of records is known.
     *   <li>For example 2: After returning a record in block 3 in a block-compressed file
     *       consisting of 5 blocks, this value should be 3 (since blocks 4 and 5 can be processed
     *       in parallel by new readers produced via dynamic work rebalancing, while the current
     *       reader continues processing block 3) if the total number of blocks is known.
     *   <li>For example (3): a reader for any non-empty unsplittable input, should return 1 until
     *       it is finished, at which point it should return 0.
     *   <li>For any reader: After returning the last split point in a file (e.g., the last record
     *       in example (1), the first record in the last block for example (2), or the first record
     *       in the file for example (3), this value should be 1: apart from the current task, no
     *       additional remainder can be split off.
     * </ul>
     *
     * <p>Defaults to {@link #SPLIT_POINTS_UNKNOWN}. Any value less than 0 will be interpreted as
     * unknown.
     *
     * <h3>Thread safety</h3>
     *
     * See the javadoc on {@link BoundedReader} for information about thread safety.
     *
     * @see #getSplitPointsConsumed()
     */
    public long getSplitPointsRemaining() {
      return SPLIT_POINTS_UNKNOWN;
    }

    /**
     * Returns a {@code Source} describing the same input that this {@code Reader} currently reads
     * (including items already read).
     *
     * <h3>Usage</h3>
     *
     * <p>Reader subclasses can use this method for convenience to access unchanging properties of
     * the source being read. Alternatively, they can cache these properties in the constructor.
     *
     * <p>The framework will call this method in the course of dynamic work rebalancing, e.g. after
     * a successful {@link BoundedSource.BoundedReader#splitAtFraction} call.
     *
     * <h3>Mutability and thread safety</h3>
     *
     * <p>Remember that {@link Source} objects must always be immutable. However, the return value
     * of this function may be affected by dynamic work rebalancing, happening asynchronously via
     * {@link BoundedSource.BoundedReader#splitAtFraction}, meaning it can return a different {@link
     * Source} object. However, the returned object itself will still itself be immutable. Callers
     * must take care not to rely on properties of the returned source that may be asynchronously
     * changed as a result of this process (e.g. do not cache an end offset when reading a file).
     *
     * <h3>Implementation</h3>
     *
     * <p>For convenience, subclasses should usually return the most concrete subclass of {@link
     * Source} possible. In practice, the implementation of this method should nearly always be one
     * of the following:
     *
     * <ul>
     *   <li>Source that inherits from a base class that already implements {@link
     *       #getCurrentSource}: delegate to base class. In this case, it is almost always an error
     *       for the subclass to maintain its own copy of the source.
     *       <pre>{@code
     * public FooReader(FooSource<T> source) {
     *   super(source);
     * }
     *
     * public FooSource<T> getCurrentSource() {
     *   return (FooSource<T>)super.getCurrentSource();
     * }
     * }</pre>
     *   <li>Source that does not support dynamic work rebalancing: return a private final variable.
     *       <pre>{@code
     * private final FooSource<T> source;
     *
     * public FooReader(FooSource<T> source) {
     *   this.source = source;
     * }
     *
     * public FooSource<T> getCurrentSource() {
     *   return source;
     * }
     * }</pre>
     *   <li>{@link BoundedSource.BoundedReader} that explicitly supports dynamic work rebalancing:
     *       maintain a variable pointing to an immutable source object, and protect it with
     *       synchronization.
     *       <pre>{@code
     * private FooSource<T> source;
     *
     * public FooReader(FooSource<T> source) {
     *   this.source = source;
     * }
     *
     * public synchronized FooSource<T> getCurrentSource() {
     *   return source;
     * }
     *
     * public synchronized FooSource<T> splitAtFraction(double fraction) {
     *   ...
     *   FooSource<T> primary = ...;
     *   FooSource<T> residual = ...;
     *   this.source = primary;
     *   return residual;
     * }
     * }</pre>
     * </ul>
     */
    @Override
    public abstract BoundedSource<T> getCurrentSource();

    /**
     * Tells the reader to narrow the range of the input it's going to read and give up the
     * remainder, so that the new range would contain approximately the given fraction of the amount
     * of data in the current range.
     *
     * <p>Returns a {@code BoundedSource} representing the remainder.
     *
     * <h3>Detailed description</h3>
     *
     * Assuming the following sequence of calls:
     *
     * <pre>{@code
     * BoundedSource<T> initial = reader.getCurrentSource();
     * BoundedSource<T> residual = reader.splitAtFraction(fraction);
     * BoundedSource<T> primary = reader.getCurrentSource();
     * }</pre>
     *
     * <ul>
     *   <li>The "primary" and "residual" sources, when read, should together cover the same set of
     *       records as "initial".
     *   <li>The current reader should continue to be in a valid state, and continuing to read from
     *       it should, together with the records it already read, yield the same records as would
     *       have been read by "primary".
     *   <li>The amount of data read by "primary" should ideally represent approximately the given
     *       fraction of the amount of data read by "initial".
     * </ul>
     *
     * For example, a reader that reads a range of offsets <i>[A, B)</i> in a file might implement
     * this method by truncating the current range to <i>[A, A + fraction*(B-A))</i> and returning a
     * Source representing the range <i>[A + fraction*(B-A), B)</i>.
     *
     * <p>This method should return {@code null} if the split cannot be performed for this fraction
     * while satisfying the semantics above. E.g., a reader that reads a range of offsets in a file
     * should return {@code null} if it is already past the position in its range corresponding to
     * the given fraction. In this case, the method MUST have no effect (the reader must behave as
     * if the method hadn't been called at all).
     *
     * <h3>Statefulness</h3>
     *
     * Since this method (if successful) affects the reader's source, in subsequent invocations
     * "fraction" should be interpreted relative to the new current source.
     *
     * <h3>Thread safety and blocking</h3>
     *
     * This method will be called concurrently to other methods (however there will not be multiple
     * concurrent invocations of this method itself), and it is critical for it to be implemented in
     * a thread-safe way (otherwise data loss is possible).
     *
     * <p>It is also very important that this method always completes quickly. In particular, it
     * should not perform or wait on any blocking operations such as I/O, RPCs etc. Violating this
     * requirement may stall completion of the work item or even cause it to fail.
     *
     * <p>It is incorrect to make both this method and {@link #start}/{@link #advance} {@code
     * synchronized}, because those methods can perform blocking operations, and then this method
     * would have to wait for those calls to complete.
     *
     * <p>{@link org.apache.beam.sdk.io.range.RangeTracker} makes it easy to implement this method
     * safely and correctly.
     *
     * <p>By default, returns null to indicate that splitting is not possible.
     */
    public @Nullable BoundedSource<T> splitAtFraction(double fraction) {
      return null;
    }

    /** By default, returns the minimum possible timestamp. */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }
}
