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

import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.math.RoundingMode;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.DoubleMath;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Creates an {@link IsmSink} from a {@link CloudObject} spec. Note that it is invalid to use a non
 * {@link IsmRecordCoder} with this sink factory.
 */
public class IsmSinkFactory implements SinkFactory {

  /** A {@link SinkFactory.Registrar} for ISM sinks. */
  @AutoService(SinkFactory.Registrar.class)
  public static class Registrar implements SinkFactory.Registrar {

    @Override
    public Map<String, SinkFactory> factories() {
      IsmSinkFactory factory = new IsmSinkFactory();
      return ImmutableMap.of(
          "IsmSink", factory, "org.apache.beam.runners.dataflow.worker.IsmSink", factory);
    }
  }

  // Limit Bloom filters to be at most 0.1% of the worker cache size each
  private static final double BLOOM_FILTER_SIZE_LIMIT_MULTIPLIER = 0.001;
  private static final long MIN_BLOOM_FILTER_SIZE_BYTES = 128L;

  @Override
  public Sink<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    options = checkArgumentNotNull(options);
    coder = checkArgumentNotNull(coder);

    // The validity of this coder is checked in detail by the typed create, below
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<IsmRecord<Object>>> typedCoder =
        (Coder<WindowedValue<IsmRecord<Object>>>) coder;

    String filename = getString(spec, WorkerPropertyNames.FILENAME);

    checkArgument(
        typedCoder instanceof WindowedValueCoder,
        "%s only supports using %s but got %s.",
        IsmSink.class,
        WindowedValueCoder.class,
        typedCoder);
    WindowedValueCoder<IsmRecord<Object>> windowedCoder =
        (WindowedValueCoder<IsmRecord<Object>>) typedCoder;

    checkArgument(
        windowedCoder.getValueCoder() instanceof IsmRecordCoder,
        "%s only supports using %s but got %s.",
        IsmSink.class,
        IsmRecordCoder.class,
        windowedCoder.getValueCoder());
    @SuppressWarnings("unchecked")
    IsmRecordCoder<Object> ismCoder = (IsmRecordCoder<Object>) windowedCoder.getValueCoder();

    long bloomFilterSizeLimitBytes =
        Math.max(
            MIN_BLOOM_FILTER_SIZE_BYTES,
            DoubleMath.roundToLong(
                BLOOM_FILTER_SIZE_LIMIT_MULTIPLIER
                    * options.as(DataflowWorkerHarnessOptions.class).getWorkerCacheMb()
                    // Note the conversion from MiB to bytes
                    * 1024
                    * 1024,
                RoundingMode.DOWN));

    return new IsmSink<>(
        FileSystems.matchNewResource(filename, false), ismCoder, bloomFilterSizeLimitBytes);
  }
}
