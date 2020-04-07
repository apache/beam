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
package org.apache.beam.sdk.extensions.ml;

import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Parent class for transform utilizing Cloud Vision API.
 *
 * @param <T> Type of input PCollection.
 */
public abstract class AnnotateImages<T>
    extends PTransform<PCollection<T>, PCollection<List<AnnotateImageResponse>>> {

  private static final Long MIN_BATCH_SIZE = 1L;
  private static final Long MAX_BATCH_SIZE = 5L;

  protected final PCollectionView<Map<T, ImageContext>> contextSideInput;
  protected final List<Feature> featureList;
  private long batchSize;

  public AnnotateImages(
      PCollectionView<Map<T, ImageContext>> contextSideInput,
      List<Feature> featureList,
      long batchSize) {
    this.contextSideInput = contextSideInput;
    this.featureList = featureList;
    checkBatchSizeCorrectness(batchSize);
    this.batchSize = batchSize;
  }

  public AnnotateImages(List<Feature> featureList, long batchSize) {
    contextSideInput = null;
    this.featureList = featureList;
    checkBatchSizeCorrectness(batchSize);
    this.batchSize = batchSize;
  }

  private void checkBatchSizeCorrectness(long batchSize) {
    if (batchSize > MAX_BATCH_SIZE) {
      throw new IllegalArgumentException(
          String.format(
              "Max batch size exceeded.%n" + "Batch size needs to be equal or smaller than %d",
              MAX_BATCH_SIZE));
    } else if (batchSize < MIN_BATCH_SIZE) {
      throw new IllegalArgumentException(
          String.format(
              "Min batch size not reached.%n" + "Batch size needs to be larger than %d",
              MIN_BATCH_SIZE));
    }
  }

  /**
   * Applies all necessary transforms to call the Vision API. In order to group requests into
   * batches, we assign keys to the requests, as {@link GroupIntoBatches} works only on {@link KV}s.
   */
  @Override
  public PCollection<List<AnnotateImageResponse>> expand(PCollection<T> input) {
    ParDo.SingleOutput<T, AnnotateImageRequest> inputToRequestMapper;
    if (contextSideInput != null) {
      inputToRequestMapper =
          ParDo.of(new MapInputToRequest(contextSideInput)).withSideInputs(contextSideInput);
    } else {
      inputToRequestMapper = ParDo.of(new MapInputToRequest(null));
    }
    return input
        .apply(inputToRequestMapper)
        .apply(ParDo.of(new AssignRandomKeys()))
        .apply(GroupIntoBatches.ofSize(batchSize))
        .apply(ParDo.of(new ExtractValues()))
        .apply(ParDo.of(new PerformImageAnnotation()));
  }

  /**
   * Input type to {@link AnnotateImageRequest} mapper. Needs to be implemented by child classes
   *
   * @param input Input element.
   * @param ctx optional image context.
   * @return A valid {@link AnnotateImageRequest} object.
   */
  public abstract AnnotateImageRequest mapToRequest(T input, ImageContext ctx);

  /**
   * The {@link DoFn} performing the calls to Cloud Vision API. Input PCollection contains lists of
   * {@link AnnotateImageRequest}s ready for batching.
   */
  public static class PerformImageAnnotation
      extends DoFn<List<AnnotateImageRequest>, List<AnnotateImageResponse>> {

    private transient ImageAnnotatorClient imageAnnotatorClient;

    public PerformImageAnnotation() {}

    /**
     * Parametrized constructor to make mock injection easier in testing.
     *
     * @param imageAnnotatorClient
     */
    public PerformImageAnnotation(ImageAnnotatorClient imageAnnotatorClient) {
      this.imageAnnotatorClient = imageAnnotatorClient;
    }

    @StartBundle
    public void startBundle() throws IOException {
      imageAnnotatorClient = ImageAnnotatorClient.create();
    }

    @Teardown
    public void teardown() {
      imageAnnotatorClient.close();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(getResponse(context.element()));
    }

    /**
     * Performs the call itself. Default access for testing.
     *
     * @param requests request list.
     * @return response list.
     */
    List<AnnotateImageResponse> getResponse(List<AnnotateImageRequest> requests) {
      BatchAnnotateImagesResponse batchAnnotateImagesResponse =
          imageAnnotatorClient.batchAnnotateImages(requests);
      return batchAnnotateImagesResponse.getResponsesList();
    }
  }

  /** A transform that converts input elements to {@link KV}s for grouping. */
  private static class AssignRandomKeys
      extends DoFn<AnnotateImageRequest, KV<Long, AnnotateImageRequest>> {
    private Random random;

    @Setup
    public void setup() {
      random = new Random();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(KV.of(random.nextLong(), context.element()));
    }
  }

  /** A transform extracting lists of values from groups. */
  private static class ExtractValues
      extends DoFn<KV<Long, Iterable<AnnotateImageRequest>>, List<AnnotateImageRequest>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Iterable<AnnotateImageRequest> value = context.element().getValue();
      ArrayList<AnnotateImageRequest> annotateImageRequests = new ArrayList<>();
      value.forEach(annotateImageRequests::add);
      context.output(annotateImageRequests);
    }
  }

  /** Transform using an implementation of the mapToRequest function. */
  private class MapInputToRequest extends DoFn<T, AnnotateImageRequest> {
    PCollectionView<Map<T, ImageContext>> sideInput;

    public MapInputToRequest(PCollectionView<Map<T, ImageContext>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (sideInput != null) {
        Map<T, ImageContext> imageContextMap = context.sideInput(sideInput);
        context.output(mapToRequest(context.element(), imageContextMap.get(context.element())));
      } else {
        context.output(mapToRequest(context.element(), null));
      }
    }
  }
}
