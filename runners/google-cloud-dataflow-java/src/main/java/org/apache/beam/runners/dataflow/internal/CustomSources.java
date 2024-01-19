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
package org.apache.beam.runners.dataflow.internal;

import static com.google.api.client.util.Base64.encodeBase64String;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.util.Structs.addStringList;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.SourceMetadata;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for supporting sources defined as {@code Source}.
 *
 * <p>Provides a bridge between the high-level {@code Source} API and the low-level {@code
 * CloudSource} class.
 */
public class CustomSources {
  private static final String SERIALIZED_SOURCE = "serialized_source";
  @VisibleForTesting static final String SERIALIZED_SOURCE_SPLITS = "serialized_source_splits";

  private static final Logger LOG = LoggerFactory.getLogger(CustomSources.class);

  private static int getDesiredNumUnboundedSourceSplits(DataflowPipelineOptions options) {
    if (options.getDesiredNumUnboundedSourceSplits() > 0) {
      return options.getDesiredNumUnboundedSourceSplits();
    }

    int cores = 4; // TODO: decide at runtime?
    if (options.getMaxNumWorkers() > 0) {
      return options.getMaxNumWorkers() * cores;
    } else if (options.getNumWorkers() > 0) {
      return options.getNumWorkers() * cores;
    } else {
      return 5 * cores;
    }
  }

  public static com.google.api.services.dataflow.model.Source serializeToCloudSource(
      Source<?> source, PipelineOptions options) throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    // We ourselves act as the SourceFormat.
    cloudSource.setSpec(CloudObject.forClass(CustomSources.class));
    addString(
        cloudSource.getSpec(), SERIALIZED_SOURCE, encodeBase64String(serializeToByteArray(source)));

    SourceMetadata metadata = new SourceMetadata();
    if (source instanceof BoundedSource) {
      BoundedSource<?> boundedSource = (BoundedSource<?>) source;

      // Size estimation is best effort so we continue even if it fails here.
      try {
        metadata.setEstimatedSizeBytes(boundedSource.getEstimatedSizeBytes(options));
      } catch (Exception e) {
        LOG.warn("Size estimation of the source failed: " + source, e);
      }
    } else if (source instanceof UnboundedSource) {
      UnboundedSource<?, ?> unboundedSource = (UnboundedSource<?, ?>) source;
      metadata.setInfinite(true);
      List<String> encodedSplits = new ArrayList<>();
      int desiredNumSplits =
          getDesiredNumUnboundedSourceSplits(options.as(DataflowPipelineOptions.class));
      for (UnboundedSource<?, ?> split : unboundedSource.split(desiredNumSplits, options)) {
        encodedSplits.add(encodeBase64String(serializeToByteArray(split)));
      }
      checkArgument(!encodedSplits.isEmpty(), "UnboundedSources must have at least one split");
      addStringList(cloudSource.getSpec(), SERIALIZED_SOURCE_SPLITS, encodedSplits);
    } else {
      throw new IllegalArgumentException("Unexpected source kind: " + source.getClass());
    }

    cloudSource.setMetadata(metadata);
    return cloudSource;
  }
}
