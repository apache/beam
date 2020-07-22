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

import static com.google.api.client.util.Base64.decodeBase64;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.runners.dataflow.worker.ShuffleSink.parseShuffleKind;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Creates a {@link ShuffleSink} from a {@link CloudObject} spec. */
public class ShuffleSinkFactory implements SinkFactory {

  /** A {@link SinkFactory.Registrar} for shuffle sinks. */
  @AutoService(SinkFactory.Registrar.class)
  public static class Registrar implements SinkFactory.Registrar {

    @Override
    public Map<String, SinkFactory> factories() {
      ShuffleSinkFactory factory = new ShuffleSinkFactory();
      return ImmutableMap.of(
          "ShuffleSink",
          factory,
          "org.apache.beam.runners.dataflow.worker.runners.worker.ShuffleSink",
          factory);
    }
  }

  @Override
  public ShuffleSink<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    coder = checkArgumentNotNull(coder);
    executionContext = checkArgumentNotNull(executionContext);

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<Object>> typedCoder = (Coder<WindowedValue<Object>>) coder;

    return new ShuffleSink<>(
        options,
        decodeBase64(getString(spec, WorkerPropertyNames.SHUFFLE_WRITER_CONFIG, null)),
        parseShuffleKind(getString(spec, WorkerPropertyNames.SHUFFLE_KIND)),
        typedCoder,
        (BatchModeExecutionContext) executionContext,
        operationContext);
  }
}
