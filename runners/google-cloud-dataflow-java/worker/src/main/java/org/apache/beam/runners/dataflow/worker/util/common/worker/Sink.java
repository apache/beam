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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.IOException;

/**
 * Abstract base class for Sinks.
 *
 * <p>A Sink is written to by getting a SinkWriter and adding values to it.
 *
 * @param <T> the type of the elements written to the sink
 */
public abstract class Sink<T> {

  /** Returns a Writer that allows writing to this Sink. */
  public abstract SinkWriter<T> writer() throws IOException;

  /** Writes to a Sink. */
  public interface SinkWriter<ElemT> extends AutoCloseable {
    /** Adds a value to the sink. Returns the size in bytes of the data written. */
    public long add(ElemT value) throws IOException;

    /**
     * {@inheritDoc}
     *
     * <p>{@link #close()} will not be called after a call to {@link #abort()}
     */
    @Override
    public void close() throws IOException;

    /**
     * Aborts writing to this sink. The sink's output will be ignored.
     *
     * <p>{@link #abort()} will not be called after a call to {@link #close()}.
     */
    public void abort() throws IOException;
  }

  /** Returns whether this Sink can be restarted. */
  public boolean supportsRestart() {
    return false;
  }
}
