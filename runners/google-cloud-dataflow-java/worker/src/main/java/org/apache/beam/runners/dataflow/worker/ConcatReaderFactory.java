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

import static org.apache.beam.runners.dataflow.util.Structs.getBoolean;
import static org.apache.beam.runners.dataflow.util.Structs.getListOfMaps;
import static org.apache.beam.runners.dataflow.util.Structs.getLong;
import static org.apache.beam.runners.dataflow.util.Structs.getObject;

import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SourceMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Creates an {@link ConcatReader} from a {@link CloudObject} spec. */
public class ConcatReaderFactory implements ReaderFactory {

  private final ReaderRegistry registry;

  private ConcatReaderFactory(ReaderRegistry registry) {
    this.registry = registry;
  }

  /**
   * Returns a new {@link ConcatReaderFactory} that will use the default {@link ReaderRegistry} to
   * create sub-{@link NativeReader} instances.
   */
  public static ConcatReaderFactory withDefaultRegistry() {
    return withRegistry(ReaderRegistry.defaultRegistry());
  }

  /**
   * Returns a new {@link ConcatReaderFactory} that will use the provided {@link ReaderRegistry} to
   * create sub-{@link NativeReader} instances.
   */
  public static ConcatReaderFactory withRegistry(ReaderRegistry registry) {
    return new ConcatReaderFactory(registry);
  }

  @Override
  public NativeReader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    @SuppressWarnings("unchecked")
    Coder<Object> typedCoder = (Coder<Object>) coder;
    return createTyped(spec, typedCoder, options, executionContext, operationContext);
  }

  public <T> NativeReader<T> createTyped(
      CloudObject spec,
      @Nullable Coder<T> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    List<Source> sources = getSubSources(spec);
    return new ConcatReader<T>(registry, options, executionContext, operationContext, sources);
  }

  private static List<Source> getSubSources(CloudObject spec) throws Exception {
    List<Source> subSources = new ArrayList<>();

    // Get the list of sub-sources.
    List<Map<String, Object>> subSourceDictionaries =
        getListOfMaps(spec, WorkerPropertyNames.CONCAT_SOURCE_SOURCES, null);
    if (subSourceDictionaries == null) {
      return subSources;
    }

    for (Map<String, Object> subSourceDictionary : subSourceDictionaries) {
      // Each sub-source is encoded as a dictionary that contains several properties.
      subSources.add(createSourceFromDictionary(subSourceDictionary));
    }

    return subSources;
  }

  public static Source createSourceFromDictionary(Map<String, Object> dictionary) throws Exception {
    Source source = new Source();

    // Set spec
    CloudObject subSourceSpec =
        CloudObject.fromSpec(getObject(dictionary, PropertyNames.SOURCE_SPEC));
    source.setSpec(subSourceSpec);

    // Set encoding
    CloudObject subSourceEncoding =
        CloudObject.fromSpec(getObject(dictionary, PropertyNames.ENCODING, null));
    if (subSourceEncoding != null) {
      source.setCodec(subSourceEncoding);
    }

    // Set base specs
    List<Map<String, Object>> subSourceBaseSpecs =
        getListOfMaps(dictionary, WorkerPropertyNames.CONCAT_SOURCE_BASE_SPECS, null);
    if (subSourceBaseSpecs != null) {
      source.setBaseSpecs(subSourceBaseSpecs);
    }

    // Set metadata
    SourceMetadata metadata = new SourceMetadata();
    Boolean infinite = getBoolean(dictionary, PropertyNames.SOURCE_IS_INFINITE, null);
    if (infinite != null) {
      metadata.setInfinite(infinite);
    }
    Long estimatedSizeBytes = getLong(dictionary, PropertyNames.SOURCE_ESTIMATED_SIZE_BYTES, null);
    if (estimatedSizeBytes != null) {
      metadata.setEstimatedSizeBytes(estimatedSizeBytes);
    }
    if (estimatedSizeBytes != null || infinite != null) {
      source.setMetadata(metadata);
    }

    // Set doesNotNeedSplitting
    Boolean doesNotNeedSplitting =
        getBoolean(dictionary, PropertyNames.SOURCE_DOES_NOT_NEED_SPLITTING, null);
    if (doesNotNeedSplitting != null) {
      source.setDoesNotNeedSplitting(doesNotNeedSplitting);
    }

    return source;
  }
}
