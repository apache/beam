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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A wrapper for Sink that reports bytes buffered (or written) to {@link DataflowExecutionContext}.
 * The current use case is for {@link StreamingModeExecutionContext} to limit processing when the
 * amount of data buffered is large.
 */
public class SizeReportingSinkWrapper<T> extends Sink<T> {

  private final Sink<T> underlyingSink;
  private final DataflowExecutionContext<?> executionContext;

  SizeReportingSinkWrapper(Sink<T> underlyingSink, DataflowExecutionContext<?> executionContext) {
    this.underlyingSink = underlyingSink;
    this.executionContext = executionContext;
  }

  @VisibleForTesting
  Sink<T> getUnderlyingSink() {
    return underlyingSink;
  }

  @Override
  public SinkWriter<T> writer() throws IOException {
    return new SizeLimitingWriterWrapper(underlyingSink.writer());
  }

  @Override
  public boolean supportsRestart() {
    return underlyingSink.supportsRestart();
  }

  class SizeLimitingWriterWrapper implements SinkWriter<T> {
    private final SinkWriter<T> underlyingWriter;

    SizeLimitingWriterWrapper(SinkWriter<T> underlyingSinkWriter) {
      this.underlyingWriter = underlyingSinkWriter;
    }

    @Override
    public long add(T value) throws IOException {
      long size = underlyingWriter.add(value);
      executionContext.reportBytesSinked(size);
      return size;
    }

    @Override
    public void close() throws IOException {
      underlyingWriter.close();
    }

    @Override
    public void abort() throws IOException {
      underlyingWriter.abort();
    }
  }
}
