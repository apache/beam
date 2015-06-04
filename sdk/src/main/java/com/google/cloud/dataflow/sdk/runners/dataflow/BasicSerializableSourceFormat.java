/*
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
import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.DerivedSource;
import com.google.api.services.dataflow.model.DynamicSourceSplit;
import com.google.api.services.dataflow.model.SourceGetMetadataRequest;
import com.google.api.services.dataflow.model.SourceGetMetadataResponse;
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitOptions;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
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
 * <p>
 * Provides a bridge between the high-level {@code Source} API and the raw
 * API-level {@code SourceFormat} API, by encoding the serialized
 * {@code Source} in a parameter of the API {@code Source} message.
 */
public class BasicSerializableSourceFormat implements SourceFormat {
  private static final String SERIALIZED_SOURCE = "serialized_source";
  private static final long DEFAULT_DESIRED_BUNDLE_SIZE_BYTES = 64 * (1 << 20);

  private static final Logger LOG = LoggerFactory.getLogger(BasicSerializableSourceFormat.class);

  private final PipelineOptions options;

  public BasicSerializableSourceFormat(PipelineOptions options) {
    this.options = options;
  }

  /**
   * A {@code DynamicSplitResult} specified explicitly by a pair of {@code BoundedSource}
   * objects describing the primary and residual sources.
   */
  public static final class BoundedSourceSplit<T> implements Reader.DynamicSplitResult {
    public final BoundedSource<T> primary;
    public final BoundedSource<T> residual;

    public BoundedSourceSplit(BoundedSource<T> primary, BoundedSource<T> residual) {
      this.primary = primary;
      this.residual = residual;
    }
  }

  public static DynamicSourceSplit toSourceSplit(
      BoundedSourceSplit<?> sourceSplitResult, PipelineOptions options) {
    DynamicSourceSplit sourceSplit = new DynamicSourceSplit();
    com.google.api.services.dataflow.model.Source primarySource;
    com.google.api.services.dataflow.model.Source residualSource;
    try {
      primarySource = serializeToCloudSource(sourceSplitResult.primary, options);
      residualSource = serializeToCloudSource(sourceSplitResult.residual, options);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize one of the parts of the source split", e);
    }
    sourceSplit.setPrimary(
        new DerivedSource()
            .setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT")
            .setSource(primarySource));
    sourceSplit.setResidual(
        new DerivedSource()
            .setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT")
            .setSource(residualSource));
    return sourceSplit;
  }

  /**
   * Executes a protocol-level split {@code SourceOperationRequest} for bounded sources
   * by deserializing its source to a {@code BoundedSource}, splitting it, and
   * serializing results back.
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
   * Factory method allowing this class to satisfy the implicit contract of
   * {@link com.google.cloud.dataflow.sdk.runners.worker.ReaderFactory}.
   */
  public static <T> Reader<WindowedValue<T>> create(
      final PipelineOptions options, CloudObject spec, Coder<WindowedValue<T>> coder,
      final ExecutionContext executionContext) throws Exception {
    // The parameter "coder" is deliberately never used. It is an artifact of ReaderFactory:
    // some readers need a coder, some don't (i.e. for some it doesn't even make sense),
    // but ReaderFactory passes it to all readers anyway.
    @SuppressWarnings("unchecked")
    final Source<T> source = (Source<T>) deserializeFromCloudSource(spec);
    return new Reader<WindowedValue<T>>() {
      @Override
      public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
        return new BasicSerializableSourceFormat.ReaderIterator<>(
            source.createReader(options, executionContext));
      }
    };
  }

  private SourceSplitResponse performSplit(SourceSplitRequest request) throws Exception {
    Source<?> anySource = deserializeFromCloudSource(request.getSource().getSpec());
    if (!(anySource instanceof BoundedSource)) {
      throw new UnsupportedOperationException("Cannot split a non-Bounded source: " + anySource);
    }
    BoundedSource<?> source = (BoundedSource<?>) anySource;
    LOG.debug("Splitting source: {}", source);

    // Produce simple independent, unsplittable bundles with no metadata attached.
    SourceSplitResponse response = new SourceSplitResponse();
    response.setBundles(new ArrayList<DerivedSource>());
    SourceSplitOptions splitOptions = request.getOptions();
    Long desiredBundleSizeBytes =
        (splitOptions == null) ? null : splitOptions.getDesiredBundleSizeBytes();
    if (desiredBundleSizeBytes == null) {
      desiredBundleSizeBytes = DEFAULT_DESIRED_BUNDLE_SIZE_BYTES;
    }
    List<? extends BoundedSource<?>> bundles =
        source.splitIntoBundles(desiredBundleSizeBytes, options);
    LOG.debug("Splitting produced {} bundles", bundles.size());
    for (BoundedSource<?> split : bundles) {
      try {
        split.validate();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Splitting a valid source produced an invalid bundle. "
                + "\nOriginal source: "
                + source
                + "\nInvalid bundle: "
                + split,
            e);
      }
      DerivedSource bundle = new DerivedSource();

      com.google.api.services.dataflow.model.Source cloudSource =
          serializeToCloudSource(split, options);
      cloudSource.setDoesNotNeedSplitting(true);

      bundle.setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT");
      bundle.setSource(cloudSource);
      response.getBundles().add(bundle);
    }
    response.setOutcome("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED");
    return response;
  }

  private SourceGetMetadataResponse performGetMetadata(SourceGetMetadataRequest request)
      throws Exception {
    Source<?> source = deserializeFromCloudSource(request.getSource().getSpec());
    SourceMetadata metadata = new SourceMetadata();
    if (source instanceof BoundedSource) {
      BoundedSource<?> boundedSource = (BoundedSource<?>) source;
      metadata.setProducesSortedKeys(boundedSource.producesSortedKeys(options));
      metadata.setEstimatedSizeBytes(boundedSource.getEstimatedSizeBytes(options));
    }
    SourceGetMetadataResponse response = new SourceGetMetadataResponse();
    response.setMetadata(metadata);
    return response;
  }

  public static Source<?> deserializeFromCloudSource(Map<String, Object> spec) throws Exception {
    Source<?> source = (Source<?>) deserializeFromByteArray(
        Base64.decodeBase64(getString(spec, SERIALIZED_SOURCE)), "Source");
    try {
      source.validate();
    } catch (Exception e) {
      LOG.error("Invalid source: " + source, e);
      throw e;
    }
    return source;
  }

  static com.google.api.services.dataflow.model.Source serializeToCloudSource(
      Source<?> source, PipelineOptions options) throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    // We ourselves act as the SourceFormat.
    cloudSource.setSpec(CloudObject.forClass(BasicSerializableSourceFormat.class));
    addString(
        cloudSource.getSpec(), SERIALIZED_SOURCE, encodeBase64String(serializeToByteArray(source)));

    SourceMetadata metadata = new SourceMetadata();
    if (source instanceof BoundedSource) {
      BoundedSource<?> boundedSource = (BoundedSource<?>) source;
      try {
        metadata.setProducesSortedKeys(boundedSource.producesSortedKeys(options));
      } catch (Exception e) {
        LOG.warn("Failed to check if the source produces sorted keys: " + source, e);
      }

      // Size estimation is best effort so we continue even if it fails here.
      try {
        metadata.setEstimatedSizeBytes(boundedSource.getEstimatedSizeBytes(options));
      } catch (Exception e) {
        LOG.warn("Size estimation of the source failed: " + source, e);
      }
    }

    cloudSource.setMetadata(metadata);
    return cloudSource;
  }

  public static <T> void evaluateReadHelper(
      Read.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    try {
      List<DirectPipelineRunner.ValueWithMetadata<T>> output = new ArrayList<>();
      Source<T> anySource = transform.getSource();
      if (!(anySource instanceof BoundedSource)) {
        throw new IllegalArgumentException("Unexpected read from a user-defined unbounded source");
      }
      BoundedSource<T> source = (BoundedSource<T>) anySource;
      try (BoundedSource.BoundedReader<T> reader =
          source.createReader(context.getPipelineOptions(), null)) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          output.add(
              DirectPipelineRunner.ValueWithMetadata.of(
                  WindowedValue.of(
                      reader.getCurrent(), reader.getCurrentTimestamp(), GlobalWindow.INSTANCE)));
        }
      }
      context.setPCollectionValuesWithMetadata(context.getOutput(transform), output);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> void translateReadHelper(
      Read.Bound<T> transform, DataflowPipelineTranslator.TranslationContext context) {
    try {
      Source<T> anySource = transform.getSource();
      if (!(anySource instanceof BoundedSource)) {
        throw new IllegalArgumentException("Unexpected read from a user-defined unbounded source");
      }
      BoundedSource<T> source = (BoundedSource<T>) anySource;
      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, PropertyNames.CUSTOM_SOURCE_FORMAT);
      context.addInput(
          PropertyNames.SOURCE_STEP_INPUT,
          cloudSourceToDictionary(serializeToCloudSource(source, context.getPipelineOptions())));
      context.addValueOnlyOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adapter from the {@code Source.Reader} interface to {@code Reader.ReaderIterator},
   * wrapping every value into the global window. Proper windowing will be assigned by the
   * subsequent Window transform.
   * <p>
   * TODO: Consider changing the API of Reader.ReaderIterator so this adapter wouldn't be needed.
   */
  private static class ReaderIterator<T> implements Reader.ReaderIterator<WindowedValue<T>> {
    private enum NextState {
      UNKNOWN_BEFORE_START,
      UNKNOWN_BEFORE_ADVANCE,
      AVAILABLE,
      FINISHED
    }
    private Source.Reader<T> reader;
    private NextState state = NextState.UNKNOWN_BEFORE_START;

    private ReaderIterator(Source.Reader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {
      switch(state) {
        case UNKNOWN_BEFORE_START:
          try {
            if (reader.start()) {
              state = NextState.AVAILABLE;
              return true;
            } else {
              state = NextState.FINISHED;
              return false;
            }
          } catch (Exception e) {
            throw new IOException(
                "Failed to start reading from source: " + reader.getCurrentSource(), e);
          }
        case UNKNOWN_BEFORE_ADVANCE:
          if (reader.advance()) {
            state = NextState.AVAILABLE;
            return true;
          } else {
            state = NextState.FINISHED;
            return false;
          }
        case AVAILABLE: return true;
        case FINISHED: return false;
        default: throw new AssertionError();
      }
    }

    @Override
    public WindowedValue<T> next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      state = NextState.UNKNOWN_BEFORE_ADVANCE;
      return WindowedValue.of(
          reader.getCurrent(), reader.getCurrentTimestamp(), GlobalWindow.INSTANCE);
    }

    @Override
    public ReaderIterator<T> copy() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public Reader.Progress getProgress() {
      if (reader instanceof BoundedSource.BoundedReader) {
        ApproximateProgress progress = new ApproximateProgress();
        Double fractionConsumed = ((BoundedSource.BoundedReader<?>) reader).getFractionConsumed();
        if (fractionConsumed != null) {
          progress.setPercentComplete(fractionConsumed.floatValue());
        }
        return SourceTranslationUtils.cloudProgressToReaderProgress(progress);
      } else {
        // Progress estimation for unbounded sources not yet supported.
        return null;
      }
    }

    @Override
    public Reader.DynamicSplitResult requestDynamicSplit(Reader.DynamicSplitRequest request) {
      if (!(reader instanceof BoundedSource.BoundedReader)) {
        throw new IllegalStateException(
            "Unexpected requestDynamicSplit on an unbounded source: " + reader.getCurrentSource()
            + ", request: " + request);
      }

      BoundedSource.BoundedReader<T> boundedReader = (BoundedSource.BoundedReader<T>) reader;
      ApproximateProgress stopPosition =
          SourceTranslationUtils.splitRequestToApproximateProgress(request);
      Float fractionConsumed = stopPosition.getPercentComplete();
      if (fractionConsumed == null) {
        // Only truncating at a fraction is currently supported.
        return null;
      }
      BoundedSource<T> original = boundedReader.getCurrentSource();
      BoundedSource<T> residual = boundedReader.splitAtFraction(fractionConsumed.doubleValue());
      if (residual == null) {
        return null;
      }
      // Try to catch some potential subclass implementation errors early.
      BoundedSource<T> primary = boundedReader.getCurrentSource();
      if (original == primary) {
        throw new IllegalStateException(
          "Successful split did not change the current source: primary is identical to original"
          + " (Source objects MUST be immutable): " + primary);
      }
      if (original == residual) {
        throw new IllegalStateException(
          "Successful split did not change the current source: residual is identical to original"
          + " (Source objects MUST be immutable): " + residual);
      }
      try {
        primary.validate();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Successful split produced an illegal primary source. "
            + "\nOriginal: " + original + "\nPrimary: " + primary + "\nResidual: " + residual);
      }
      try {
        residual.validate();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Successful split produced an illegal residual source. "
            + "\nOriginal: " + original + "\nPrimary: " + primary + "\nResidual: " + residual);
      }
      return new BoundedSourceSplit<T>(primary, residual);
    }
  }
}
