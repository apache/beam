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

import static org.apache.beam.runners.dataflow.util.Structs.getInt;
import static org.apache.beam.runners.dataflow.util.Structs.getStrings;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Creates an InMemoryReader from a CloudObject spec. */
public class InMemoryReaderFactory implements ReaderFactory {

  /** A {@link ReaderFactory.Registrar} for in memory sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      return ImmutableMap.of("InMemorySource", new InMemoryReaderFactory());
    }
  }

  @Override
  public NativeReader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    return create(spec, coder);
  }

  <T> InMemoryReader<T> create(CloudObject spec, Coder<T> coder) throws Exception {
    return new InMemoryReader<>(
        getStrings(spec, WorkerPropertyNames.ELEMENTS, Collections.<String>emptyList()),
        getInt(spec, WorkerPropertyNames.START_INDEX, null),
        getInt(spec, WorkerPropertyNames.END_INDEX, null),
        coder);
  }
}
