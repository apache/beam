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
 *
 *  EDIT BY: NopAngel | Angel Nieto (FORK)
 *
 */

package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;

/**
 * Base class for defining input formats and creating a {@code Source} for reading the input.
 *
 * <p>Designed for bounded and unbounded sources within Beam pipelines.
 *
 * @param <T> Type of elements read by the source.
 */
public abstract class Source<T> implements Serializable, HasDisplayData {

    /** Validates the source before usage in a pipeline. */
    public void validate() {}

    /**
     * Returns the {@code Coder} for data read from this source.
     * Subclasses should override this method to specify their coder.
     */
    public Coder<T> getOutputCoder() {
        return getDefaultOutputCoder();
    }

    /** @deprecated Use {@link #getOutputCoder()} instead. */
    @Deprecated
    public Coder<T> getDefaultOutputCoder() {
        try {
            if (getClass().getMethod("getOutputCoder").getDeclaringClass().equals(Source.class)) {
                throw new UnsupportedOperationException(getClass() + " needs to override getOutputCoder().");
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return getOutputCoder();
    }

    /** Provides display data; override to include custom metrics or details. */
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {}

    /**
     * Reader interface for custom input sources.
     *
     * <p>Facilitates efficient data iteration from sources like files and databases.
     *
     * @param <T> Type of elements read by the reader.
     */
    public abstract static class Reader<T> implements AutoCloseable {

        /** Initializes the reader and moves to the first record. */
        public abstract boolean start() throws IOException;

        /** Advances the reader to the next record. */
        public abstract boolean advance() throws IOException;

        /** Returns the current data item. */
        public abstract T getCurrent() throws NoSuchElementException;

        /**
         * Returns the timestamp associated with the current data item.
         * Defaults to {@code BoundedWindow.TIMESTAMP_MIN_VALUE} if unsupported.
         */
        public abstract Instant getCurrentTimestamp() throws NoSuchElementException;

        /** Closes the reader; no further operations are permitted. */
        @Override
        public abstract void close() throws IOException;

        /** Returns the {@code Source} representing the input read by this {@code Reader}. */
        public abstract Source<T> getCurrentSource
