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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Creates an {@link IsmReader} from a {@link CloudObject} spec. Note that it is invalid to use a
 * non {@link IsmRecordCoder} with this reader factory.
 */
public class IsmReaderFactory implements ReaderFactory {

  /** A {@link ReaderFactory.Registrar} for ISM sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      return ImmutableMap.of("IsmSource", new IsmReaderFactory());
    }
  }

  public IsmReaderFactory() {}

  @Override
  public NativeReader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    coder = checkArgumentNotNull(coder);
    executionContext = checkArgumentNotNull(executionContext);
    return createImpl(spec, coder, options, executionContext, operationContext);
  }

  <V> NativeReader<?> createImpl(
      CloudObject spec,
      Coder<?> coder,
      PipelineOptions options,
      DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    final ResourceId resourceId =
        FileSystems.matchNewResource(
            getString(spec, WorkerPropertyNames.FILENAME), false /* isDirectory */);

    checkArgument(
        coder instanceof WindowedValueCoder,
        "%s only supports using %s but got %s.",
        IsmReader.class,
        WindowedValueCoder.class,
        coder);
    @SuppressWarnings("unchecked")
    WindowedValueCoder<IsmRecord<V>> windowedCoder = (WindowedValueCoder<IsmRecord<V>>) coder;

    checkArgument(
        windowedCoder.getValueCoder() instanceof IsmRecordCoder,
        "%s only supports using %s but got %s.",
        IsmReader.class,
        IsmRecordCoder.class,
        windowedCoder.getValueCoder());
    @SuppressWarnings("unchecked")
    final IsmRecordCoder<V> ismCoder = (IsmRecordCoder<V>) windowedCoder.getValueCoder();

    checkArgument(
        executionContext instanceof BatchModeExecutionContext,
        "%s only supports using %s but got %s.",
        IsmReader.class,
        BatchModeExecutionContext.class,
        executionContext);
    final BatchModeExecutionContext execContext = (BatchModeExecutionContext) executionContext;

    // We use a weak reference cache to always return the single IsmReader if there already
    // is one created within this JVM for this file instead of creating a new one each time.
    // This allows us to save on initialization costs across multiple work items that access
    // the same file.
    return execContext
        .<IsmReaderKey, NativeReader<?>>getLogicalReferenceCache()
        .get(
            new IsmReaderKey(resourceId.toString()),
            () ->
                new IsmReaderImpl<V>(
                    resourceId,
                    ismCoder,
                    execContext
                        .<IsmReaderImpl.IsmShardKey,
                            WeightedValue<
                                NavigableMap<RandomAccessData, WindowedValue<IsmRecord<V>>>>>
                            getDataCache()));
  }

  /** A cache key for IsmReaders which uniquely identifies each IsmReader. */
  private static final class IsmReaderKey {
    private final String filename;

    public IsmReaderKey(String filename) {
      this.filename = filename;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(IsmReaderKey.class).add("filename", filename).toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IsmReaderKey)) {
        return false;
      }
      IsmReaderKey key = (IsmReaderKey) obj;
      return Objects.equals(filename, key.filename);
    }

    @Override
    public int hashCode() {
      return filename.hashCode();
    }
  }
}
