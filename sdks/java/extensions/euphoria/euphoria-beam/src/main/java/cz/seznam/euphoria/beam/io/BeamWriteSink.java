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
package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Write to output sink using beam.
 */
@DoFn.BoundedPerElement
public class BeamWriteSink<T> extends PTransform<PCollection<T>, PDone> {

  private final DataSink<T> sink;

  private BeamWriteSink(DataSink<T> sink) {
    this.sink = Objects.requireNonNull(sink);
  }

  public static <T> BeamWriteSink<T> wrap(DataSink<T> sink) {
    return new BeamWriteSink<>(sink);
  }

  @Override
  public PDone expand(PCollection<T> input) {
    // TODO: decide number of shards and refactor WriteFn
    input.apply(ParDo.of(new WriteFn<>(0, sink)));
    return PDone.in(input.getPipeline());
  }

  private static final class WriteFn<T> extends DoFn<T, Void> {

    private final DataSink<T> sink;
    private final int partitionId;
    Writer<T> writer = null;

    WriteFn(int partitionId, DataSink<T> sink) {
      this.partitionId = partitionId;
      this.sink = sink;
    }

    @Setup
    public void setup() {
      writer = sink.openWriter(partitionId);
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws IOException {
      T element = c.element();
      writer.write(element);
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      writer.flush();
    }

    @Teardown
    public void tearDown() throws IOException {
      writer.commit();
      writer.close();
    }
  }
}
