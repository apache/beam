/*
 * Copyright (C) 2014 Google Inc.
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
 */

package com.google.cloud.dataflow.sdk.runners.dataflow;

import static com.google.api.client.util.Base64.encodeBase64String;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceOperationResponseToSourceOperationResponse;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceToDictionary;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceOperationRequestToCloudSourceOperationRequest;
import static com.google.cloud.dataflow.sdk.util.SerializableUtils.deserializeFromByteArray;
import static com.google.cloud.dataflow.sdk.util.SerializableUtils.serializeToByteArray;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.client.util.Base64;
import com.google.api.services.dataflow.model.SourceGetMetadataRequest;
import com.google.api.services.dataflow.model.SourceGetMetadataResponse;
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitOptions;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.api.services.dataflow.model.SourceSplitShard;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.ReadSource;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A helper class for supporting sources defined as {@code Source}.
 *
 * Provides a bridge between the high-level {@code Source} API and the raw
 * API-level {@code SourceFormat} API, by encoding the serialized
 * {@code Source} in a parameter of the API {@code Source} message.
 * <p>
 */
public class BasicSerializableSourceFormat implements SourceFormat {
  private static final String SERIALIZED_SOURCE = "serialized_source";
  private static final long DEFAULT_DESIRED_SHARD_SIZE_BYTES = 64 * (1 << 20);

  private static final Logger LOG = LoggerFactory.getLogger(BasicSerializableSourceFormat.class);

  private final PipelineOptions options;

  public BasicSerializableSourceFormat(PipelineOptions options) {
    this.options = options;
  }

  /**
   * Executes a protocol-level split {@code SourceOperationRequest} by deserializing its source
   * to a {@code Source}, splitting it, and serializing results back.
   */
  @Override
  public OperationResponse performSourceOperation(OperationRequest request) throws Exception {
    SourceOperationRequest cloudRequest =
        sourceOperationRequestToCloudSourceOperationRequest(request);
    SourceOperationResponse cloudResponse = new SourceOperationResponse();
    if (cloudRequest.getGetMetadata() != null) {
      cloudResponse.setGetMetadata(performGetMetadata(cloudRequest.getGetMetadata()));
    } else if (cloudRequest.getSplit() != null) {
      cloudResponse.setSplit(performSplit(cloudRequest.getSplit()));
    } else {
      throw new UnsupportedOperationException("Unknown source operation request");
    }
    return cloudSourceOperationResponseToSourceOperationResponse(cloudResponse);
  }

  /**
   * Factory method allowing this class to satisfy the implicit contract of {@code SourceFactory}.
   */
  @SuppressWarnings("unchecked")
  public static <T> com.google.cloud.dataflow.sdk.util.common.worker.Reader create(
      final PipelineOptions options, CloudObject spec, final Coder<WindowedValue<T>> coder,
      final ExecutionContext executionContext) throws Exception {
    final Source<T> source = (Source<T>) deserializeFromCloudSource(spec);
    return new com.google.cloud.dataflow.sdk.util.common.worker.Reader() {
      @Override
      public ReaderIterator iterator() throws IOException {
        return new BasicSerializableSourceFormat.ReaderIterator<>(
            source.createWindowedReader(options, coder, executionContext));
      }
    };
  }

  private SourceSplitResponse performSplit(SourceSplitRequest request) throws Exception {
    Source<?> source = deserializeFromCloudSource(request.getSource().getSpec());

    // Produce simple independent, unsplittable shards with no metadata attached.
    SourceSplitResponse response = new SourceSplitResponse();
    response.setShards(new ArrayList<SourceSplitShard>());
    SourceSplitOptions splitOptions = request.getOptions();
    Long desiredShardSizeBytes =
        (splitOptions == null) ? null : splitOptions.getDesiredShardSizeBytes();
    if (desiredShardSizeBytes == null) {
      desiredShardSizeBytes = DEFAULT_DESIRED_SHARD_SIZE_BYTES;
    }
    for (Source split : source.splitIntoShards(desiredShardSizeBytes, options)) {
      SourceSplitShard shard = new SourceSplitShard();

      com.google.api.services.dataflow.model.Source cloudSource =
          serializeToCloudSource(split, options);
      cloudSource.setDoesNotNeedSplitting(true);

      shard.setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT");
      shard.setSource(cloudSource);
      response.getShards().add(shard);
    }
    response.setOutcome("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED");
    return response;
  }

  private SourceGetMetadataResponse performGetMetadata(SourceGetMetadataRequest request)
      throws Exception {
    Source<?> source = deserializeFromCloudSource(request.getSource().getSpec());
    SourceMetadata metadata = new SourceMetadata();
    metadata.setProducesSortedKeys(source.producesSortedKeys(options));
    metadata.setEstimatedSizeBytes(source.getEstimatedSizeBytes(options));
    SourceGetMetadataResponse response = new SourceGetMetadataResponse();
    response.setMetadata(metadata);
    return response;
  }

  private static Source<?> deserializeFromCloudSource(Map<String, Object> spec) throws Exception {
    return (Source<?>) deserializeFromByteArray(
        Base64.decodeBase64(getString(spec, SERIALIZED_SOURCE)), "Source");
  }

  static com.google.api.services.dataflow.model.Source serializeToCloudSource(
      Source source, PipelineOptions options) throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    // We ourselves act as the SourceFormat.
    cloudSource.setSpec(CloudObject.forClass(BasicSerializableSourceFormat.class));
    addString(
        cloudSource.getSpec(), SERIALIZED_SOURCE, encodeBase64String(serializeToByteArray(source)));

    SourceMetadata metadata = new SourceMetadata();
    metadata.setProducesSortedKeys(source.producesSortedKeys(options));

    // Size estimation is best effort so we continue even if it fails here.
    try {
      metadata.setEstimatedSizeBytes(source.getEstimatedSizeBytes(options));
    } catch (Exception e) {
      LOG.warn("Size estimation of the source failed.", e);
    }

    cloudSource.setMetadata(metadata);
    return cloudSource;
  }

  public static <T> void evaluateReadHelper(
      ReadSource.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    try {
      List<WindowedValue<T>> elems = new ArrayList<>();
      Source<T> source = transform.getSource();
      try (Source.Reader<WindowedValue<T>> reader =
          source.createWindowedReader(context.getPipelineOptions(),
              WindowedValue.getValueOnlyCoder(source.getDefaultOutputCoder()), null)) {
        while (reader.advance()) {
          elems.add(reader.getCurrent());
        }
      }
      List<DirectPipelineRunner.ValueWithMetadata<T>> output = new ArrayList<>();
      for (WindowedValue<T> elem : elems) {
        output.add(DirectPipelineRunner.ValueWithMetadata.of(elem));
      }
      context.setPCollectionValuesWithMetadata(transform.getOutput(), output);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> void translateReadHelper(
      ReadSource.Bound<T> transform, DataflowPipelineTranslator.TranslationContext context) {
    try {
      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, PropertyNames.CUSTOM_SOURCE_FORMAT);
      context.addInput(
          PropertyNames.SOURCE_STEP_INPUT,
          cloudSourceToDictionary(
              serializeToCloudSource(transform.getSource(), context.getPipelineOptions())));
      context.addValueOnlyOutput(PropertyNames.OUTPUT, transform.getOutput());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adapter from the {@code Source.Reader} interface to
   * {@code Reader.ReaderIterator}.
   *
   * TODO: Consider changing the API of Reader.ReaderIterator so this adapter wouldn't be needed.
   */
  private static class ReaderIterator<T>
      implements com.google.cloud.dataflow.sdk.util.common.worker.Reader.ReaderIterator {
    private Source.Reader<T> reader;
    private boolean hasNext;
    private T next;
    private boolean advanced;

    private ReaderIterator(Source.Reader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (!advanced) {
        advanceInternal();
      }
      return hasNext;
    }

    @Override
    public T next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T res = this.next;
      advanceInternal();
      return res;
    }

    private void advanceInternal() throws IOException {
      try {
        hasNext = reader.advance();
        if (hasNext) {
          next = reader.getCurrent();
        }
        advanced = true;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public com.google.cloud.dataflow.sdk.util.common.worker.Reader.ReaderIterator copy()
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public Reader.Progress getProgress() {
      return null;
    }

    @Override
    public Reader.ForkResult requestFork(Reader.ForkRequest request) {
      return null;
    }
  }
}
