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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * A wrapper translating Flink sinks implementing the {@link SinkFunction} interface, into
 * unbounded Beam sinks (see {@link UnboundedSource}).
 * */
public class UnboundedFlinkSink<T> extends Sink<T> {

  /* The Flink sink function */
  private final SinkFunction<T> flinkSink;

  private UnboundedFlinkSink(SinkFunction<T> flinkSink) {
    this.flinkSink = flinkSink;
  }

  public SinkFunction<T> getFlinkSource() {
    return this.flinkSink;
  }

  @Override
  public void validate(PipelineOptions options) {
  }

  @Override
  public WriteOperation<T, ?> createWriteOperation(PipelineOptions options) {
    return new WriteOperation<T, Object>() {
      @Override
      public void initialize(PipelineOptions options) throws Exception {

      }

      @Override
      public void setWindowedWrites(boolean windowedWrites) {
      }

      @Override
      public void finalize(Iterable<Object> writerResults, PipelineOptions options)
          throws Exception {

      }

      @Override
      public Coder<Object> getWriterResultCoder() {
        return new Coder<Object>() {
          @Override
          public void encode(Object value, OutputStream outStream, Context context)
              throws CoderException, IOException {

          }

          @Override
          public Object decode(InputStream inStream, Context context)
              throws CoderException, IOException {
            return null;
          }

          @Override
          public List<? extends Coder<?>> getCoderArguments() {
            return null;
          }

          @Override
          public CloudObject asCloudObject() {
            return null;
          }

          @Override
          public void verifyDeterministic() throws NonDeterministicException {

          }

          @Override
          public boolean consistentWithEquals() {
            return false;
          }

          @Override
          public Object structuralValue(Object value) throws Exception {
            return null;
          }

          @Override
          public boolean isRegisterByteSizeObserverCheap(Object value, Context context) {
            return false;
          }

          @Override
          public void registerByteSizeObserver(Object value,
                                               ElementByteSizeObserver observer,
                                               Context context) throws Exception {

          }

          @Override
          public String getEncodingId() {
            return null;
          }

          @Override
          public Collection<String> getAllowedEncodings() {
            return null;
          }

          @Override
          public TypeDescriptor<Object> getEncodedTypeDescriptor() {
            return TypeDescriptor.of(Object.class);
          }
        };
      }

      @Override
      public Writer<T, Object> createWriter(PipelineOptions options) throws Exception {
        return new Writer<T, Object>() {
          @Override
          public void openWindowed(String uId,
                                   BoundedWindow window,
                                   PaneInfo paneInfo,
                                   int shard,
                                   int numShards) throws Exception {
          }

          @Override
          public void openUnwindowed(String uId, int shard, int numShards) throws Exception {
          }

          @Override
          public void cleanup() throws Exception {

          }

          @Override
          public void write(T value) throws Exception {

          }

          @Override
          public Object close() throws Exception {
            return null;
          }

          @Override
          public WriteOperation<T, Object> getWriteOperation() {
            return null;
          }

        };
      }

      @Override
      public Sink<T> getSink() {
        return UnboundedFlinkSink.this;
      }
    };
  }

  /**
   * Creates a Flink sink to write to using the Write API.
   * @param flinkSink The Flink sink, e.g. FlinkKafkaProducer09
   * @param <T> The input type of the sink
   * @return A Beam sink wrapping a Flink sink
   */
  public static <T> Sink<T> of(SinkFunction<T> flinkSink) {
    return new UnboundedFlinkSink<>(flinkSink);
  }
}
