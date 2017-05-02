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
package org.apache.beam.sdk.io.hdfs;

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/**
 * This class is deprecated, and only exists for HDFSFileSink.
 */
@Deprecated
public abstract class Sink<T> implements Serializable, HasDisplayData {
  /**
   * Ensures that the sink is valid and can be written to before the write operation begins. One
   * should use {@link com.google.common.base.Preconditions} to implement this method.
   */
  public abstract void validate(PipelineOptions options);

  /**
   * Returns an instance of a {@link WriteOperation} that can write to this Sink.
   */
  public abstract WriteOperation<T, ?> createWriteOperation();

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method
   * to provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {}

  /**
   * A {@link WriteOperation} defines the process of a parallel write of objects to a Sink.
   *
   * <p>The {@code WriteOperation} defines how to perform initialization and finalization of a
   * parallel write to a sink as well as how to create a {@link Sink.Writer} object that can write
   * a bundle to the sink.
   *
   * <p>Since operations in Beam may be run multiple times for redundancy or fault-tolerance,
   * the initialization and finalization defined by a WriteOperation <b>must be idempotent</b>.
   *
   * <p>{@code WriteOperation}s may be mutable; a {@code WriteOperation} is serialized after the
   * call to {@code initialize} method and deserialized before calls to
   * {@code createWriter} and {@code finalized}. However, it is not
   * reserialized after {@code createWriter}, so {@code createWriter} should not mutate the
   * state of the {@code WriteOperation}.
   *
   * <p>See {@link Sink} for more detailed documentation about the process of writing to a Sink.
   *
   * @param <T> The type of objects to write
   * @param <WriteT> The result of a per-bundle write
   */
  public abstract static class WriteOperation<T, WriteT> implements Serializable {
    /**
     * Performs initialization before writing to the sink. Called before writing begins.
     */
    public abstract void initialize(PipelineOptions options) throws Exception;

    /**
     * Indicates that the operation will be performing windowed writes.
     */
    public abstract void setWindowedWrites(boolean windowedWrites);

    /**
     * Given an Iterable of results from bundle writes, performs finalization after writing and
     * closes the sink. Called after all bundle writes are complete.
     *
     * <p>The results that are passed to finalize are those returned by bundles that completed
     * successfully. Although bundles may have been run multiple times (for fault-tolerance), only
     * one writer result will be passed to finalize for each bundle. An implementation of finalize
     * should perform clean up of any failed and successfully retried bundles.  Note that these
     * failed bundles will not have their writer result passed to finalize, so finalize should be
     * capable of locating any temporary/partial output written by failed bundles.
     *
     * <p>A best practice is to make finalize atomic. If this is impossible given the semantics
     * of the sink, finalize should be idempotent, as it may be called multiple times in the case of
     * failure/retry or for redundancy.
     *
     * <p>Note that the iteration order of the writer results is not guaranteed to be consistent if
     * finalize is called multiple times.
     *
     * @param writerResults an Iterable of results from successful bundle writes.
     */
    public abstract void finalize(Iterable<WriteT> writerResults, PipelineOptions options)
        throws Exception;

    /**
     * Creates a new {@link Sink.Writer} to write a bundle of the input to the sink.
     *
     * <p>The bundle id that the writer will use to uniquely identify its output will be passed to
     * {@link Writer#openWindowed} or {@link Writer#openUnwindowed}.
     *
     * <p>Must not mutate the state of the WriteOperation.
     */
    public abstract Writer<T, WriteT> createWriter(PipelineOptions options) throws Exception;

    /**
     * Returns the Sink that this write operation writes to.
     */
    public abstract Sink<T> getSink();

    /**
     * Returns a coder for the writer result type.
     */
    public abstract Coder<WriteT> getWriterResultCoder();
  }

  /**
   * A Writer writes a bundle of elements from a PCollection to a sink.
   * {@link Writer#openWindowed} or {@link Writer#openUnwindowed} is called before writing begins
   * and {@link Writer#close} is called after all elements in the bundle have been written.
   * {@link Writer#write} writes an element to the sink.
   *
   * <p>Note that any access to static members or methods of a Writer must be thread-safe, as
   * multiple instances of a Writer may be instantiated in different threads on the same worker.
   *
   * <p>See {@link Sink} for more detailed documentation about the process of writing to a Sink.
   *
   * @param <T> The type of object to write
   * @param <WriteT> The writer results type (e.g., the bundle's output filename, as String)
   */
  public abstract static class Writer<T, WriteT> {
    /**
     * Performs bundle initialization. For example, creates a temporary file for writing or
     * initializes any state that will be used across calls to {@link Writer#write}.
     *
     * <p>The unique id that is given to open should be used to ensure that the writer's output does
     * not interfere with the output of other Writers, as a bundle may be executed many times for
     * fault tolerance. See {@link Sink} for more information about bundle ids.
     *
     * <p>The window and paneInfo arguments are populated when windowed writes are requested.
     * shard and numbShards are populated for the case of static sharding. In cases where the
     * runner is dynamically picking sharding, shard and numShards might both be set to -1.
     */
    public abstract void openWindowed(String uId,
                                      BoundedWindow window,
                                      PaneInfo paneInfo,
                                      int shard,
                                      int numShards) throws Exception;

    /**
     * Perform bundle initialization for the case where the file is written unwindowed.
     */
    public abstract void openUnwindowed(String uId,
                                        int shard,
                                        int numShards) throws Exception;

    public abstract void cleanup() throws Exception;

    /**
     * Called for each value in the bundle.
     */
    public abstract void write(T value) throws Exception;

    /**
     * Finishes writing the bundle. Closes any resources used for writing the bundle.
     *
     * <p>Returns a writer result that will be used in the {@link Sink.WriteOperation}'s
     * finalization. The result should contain some way to identify the output of this bundle (using
     * the bundle id). {@link WriteOperation#finalize} will use the writer result to identify
     * successful writes. See {@link Sink} for more information about bundle ids.
     *
     * @return the writer result
     */
    public abstract WriteT close() throws Exception;

    /**
     * Returns the write operation this writer belongs to.
     */
    public abstract WriteOperation<T, WriteT> getWriteOperation();


  }
}
