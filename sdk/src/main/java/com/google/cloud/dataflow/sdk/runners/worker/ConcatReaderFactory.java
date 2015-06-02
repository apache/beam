/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.getListOfMaps;
import static com.google.cloud.dataflow.sdk.util.Structs.getLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getObject;

import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Creates an {@link ConcatReader} from a {@link CloudObject} spec.
 */
public class ConcatReaderFactory {
  public static <T> Reader<T> create(PipelineOptions options, CloudObject spec,
      @SuppressWarnings("unused") Coder<T> coder, ExecutionContext executionContext)
      throws Exception {
    List<Source> sources = getSubSources(spec);
    return new ConcatReader<T>(options, executionContext, sources);
  }

  private static List<Source> getSubSources(CloudObject spec) throws Exception {
    List<Source> subSources = new ArrayList<>();

    // Get the list of sub-sources.
    List<Map<String, Object>> subSourceDictionaries =
        getListOfMaps(spec, PropertyNames.CONCAT_SOURCE_SOURCES, null);
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
        getListOfMaps(dictionary, PropertyNames.CONCAT_SOURCE_BASE_SPECS, null);
    if (subSourceBaseSpecs != null) {
      source.setBaseSpecs(subSourceBaseSpecs);
    }

    // Set metadata
    SourceMetadata metadata = new SourceMetadata();
    Boolean producesSortedKeys =
        getBoolean(dictionary, PropertyNames.SOURCE_PRODUCES_SORTED_KEYS, null);
    if (producesSortedKeys != null) {
      metadata.setProducesSortedKeys(producesSortedKeys);
    }
    Boolean infinite = getBoolean(dictionary, PropertyNames.SOURCE_IS_INFINITE, null);
    if (infinite != null) {
      metadata.setInfinite(infinite);
    }
    Long estimatedSizeBytes = getLong(dictionary, PropertyNames.SOURCE_ESTIMATED_SIZE_BYTES, null);
    if (estimatedSizeBytes != null) {
      metadata.setEstimatedSizeBytes(estimatedSizeBytes);
    }
    if (producesSortedKeys != null || estimatedSizeBytes != null || infinite != null) {
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
