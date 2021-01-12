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
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageContext;
import com.google.cloud.vision.v1.ImageSource;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Factory class for implementations of {@link AnnotateImages}.
 *
 * <p>Example usage:
 *
 * <pre>
 * pipeline
 *  .apply(Create.of(IMAGE_URI))
 *  .apply(CloudVision.annotateImagesFromGcsUri(sideInputWithContext,
 *         features, 1, 1));
 * </pre>
 */
@Experimental
public class CloudVision {
  private static final int DEFAULT_PARALLELISM = 5;

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from their
   * GCS addresses.
   *
   * @param contextSideInput optional side input with contexts for select images. The {@link
   *     ImageContext} objects provide additional metadata for the annotation API. This way users
   *     can
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @param desiredRequestParallelism desired number of concurrent batched requests.
   * @return the PTransform.
   */
  public static AnnotateImagesFromGcsUri annotateImagesFromGcsUri(
      PCollectionView<Map<String, ImageContext>> contextSideInput,
      List<Feature> features,
      long batchSize,
      int desiredRequestParallelism) {
    return new AnnotateImagesFromGcsUri(
        contextSideInput, features, batchSize, desiredRequestParallelism);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from their
   * GCS addresses. Uses a default value 5 for desiredRequestParallelism.
   *
   * @param contextSideInput optional side input with contexts for select images. The {@link
   *     ImageContext} objects provide additional metadata for the annotation API. This way users
   *     can fine-tune analysis of selected images.
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @return the PTransform.
   */
  public static AnnotateImagesFromGcsUri annotateImagesFromGcsUri(
      PCollectionView<Map<String, ImageContext>> contextSideInput,
      List<Feature> features,
      long batchSize) {
    return annotateImagesFromGcsUri(contextSideInput, features, batchSize, DEFAULT_PARALLELISM);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from their
   * contents encoded in {@link ByteString}s.
   *
   * @param contextSideInput optional side input with contexts for select images. The {@link
   *     ImageContext} objects provide additional metadata for the annotation API. This way users
   *     can fine-tune analysis of selected images.
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @param desiredRequestParallelism desired number of concurrent batched requests.
   * @return the PTransform.
   */
  public static AnnotateImagesFromBytes annotateImagesFromBytes(
      PCollectionView<Map<ByteString, ImageContext>> contextSideInput,
      List<Feature> features,
      long batchSize,
      int desiredRequestParallelism) {
    return new AnnotateImagesFromBytes(
        contextSideInput, features, batchSize, desiredRequestParallelism);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from their
   * contents encoded in {@link ByteString}s. Uses a default value 5 for desiredRequestParallelism.
   *
   * @param contextSideInput optional side input with contexts for select images. The {@link
   *     ImageContext} objects provide additional metadata for the annotation API. This way users
   *     can fine-tune analysis of selected images.
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @return the PTransform.
   */
  public static AnnotateImagesFromBytes annotateImagesFromBytes(
      PCollectionView<Map<ByteString, ImageContext>> contextSideInput,
      List<Feature> features,
      long batchSize) {
    return annotateImagesFromBytes(contextSideInput, features, batchSize, DEFAULT_PARALLELISM);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from KVs of
   * their GCS addresses in Strings and {@link ImageContext} for each image.
   *
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @param desiredRequestParallelism desired number of concurrent batched requests.
   * @return the PTransform.
   */
  public static AnnotateImagesFromBytesWithContext annotateImagesFromBytesWithContext(
      List<Feature> features, long batchSize, int desiredRequestParallelism) {
    return new AnnotateImagesFromBytesWithContext(features, batchSize, desiredRequestParallelism);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from KVs of
   * their GCS addresses in Strings and {@link ImageContext} for each image. Uses a default value 5
   * for desiredRequestParallelism.
   *
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @return the PTransform.
   */
  public static AnnotateImagesFromBytesWithContext annotateImagesFromBytesWithContext(
      List<Feature> features, long batchSize) {
    return annotateImagesFromBytesWithContext(features, batchSize, DEFAULT_PARALLELISM);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from KVs of
   * their String-encoded contents and {@link ImageContext} for each image.
   *
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @param desiredRequestParallelism desired number of concurrent batched requests.
   * @return the PTransform.
   */
  public static AnnotateImagesFromGcsUriWithContext annotateImagesFromGcsUriWithContext(
      List<Feature> features, long batchSize, int desiredRequestParallelism) {
    return new AnnotateImagesFromGcsUriWithContext(features, batchSize, desiredRequestParallelism);
  }

  /**
   * Creates a {@link org.apache.beam.sdk.transforms.PTransform} that annotates images from KVs of
   * their String-encoded contents and {@link ImageContext} for each image. Uses a default value 5
   * for desiredRequestParallelism.
   *
   * @param features annotation features that should be passed to the API
   * @param batchSize request batch size to be sent to API. Max 16, at least 1.
   * @return the PTransform.
   */
  public static AnnotateImagesFromGcsUriWithContext annotateImagesFromGcsUriWithContext(
      List<Feature> features, long batchSize) {
    return annotateImagesFromGcsUriWithContext(features, batchSize, DEFAULT_PARALLELISM);
  }

  /**
   * Accepts {@link String} (image URI on GCS) with optional {@link
   * org.apache.beam.sdk.transforms.DoFn.SideInput} with a {@link Map} of {@link ImageContext} to
   * the image.
   */
  public static class AnnotateImagesFromGcsUri extends AnnotateImages<String> {

    public AnnotateImagesFromGcsUri(
        PCollectionView<Map<String, ImageContext>> contextSideInput,
        List<Feature> featureList,
        long batchSize,
        int desiredRequestParallelism) {
      super(contextSideInput, featureList, batchSize, desiredRequestParallelism);
    }

    /**
     * Maps the {@link String} with encoded image data and the optional {@link ImageContext} into an
     * {@link AnnotateImageRequest}.
     *
     * @param uri Input element.
     * @param ctx optional image context.
     * @return a valid request.
     */
    @Override
    public AnnotateImageRequest mapToRequest(String uri, @Nullable ImageContext ctx) {
      AnnotateImageRequest.Builder builder = AnnotateImageRequest.newBuilder();
      if (ctx != null) {
        builder.setImageContext(ctx);
      }
      ImageSource imgSource = ImageSource.newBuilder().setGcsImageUri(uri).build();
      return builder
          .addAllFeatures(featureList)
          .setImage(Image.newBuilder().setSource(imgSource).build())
          .build();
    }
  }

  /**
   * Accepts {@link ByteString} (encoded image contents) with optional {@link
   * org.apache.beam.sdk.transforms.DoFn.SideInput} with a {@link Map} of {@link ImageContext} to
   * the image.
   */
  public static class AnnotateImagesFromBytes extends AnnotateImages<ByteString> {

    public AnnotateImagesFromBytes(
        PCollectionView<Map<ByteString, ImageContext>> contextSideInput,
        List<Feature> featureList,
        long batchSize,
        int desiredRequestParallelism) {
      super(contextSideInput, featureList, batchSize, desiredRequestParallelism);
    }

    /**
     * Maps the {@link ByteString} with encoded image data and the optional {@link ImageContext}
     * into an {@link AnnotateImageRequest}.
     *
     * @param input Input element.
     * @param ctx optional image context.
     * @return a valid request.
     */
    @Override
    public AnnotateImageRequest mapToRequest(ByteString input, @Nullable ImageContext ctx) {
      AnnotateImageRequest.Builder builder = AnnotateImageRequest.newBuilder();
      if (ctx != null) {
        builder.setImageContext(ctx);
      }
      return builder
          .addAllFeatures(featureList)
          .setImage(Image.newBuilder().setContent(input).build())
          .build();
    }
  }

  /**
   * Accepts {@link KV}s of {@link String} (GCS URI to the image) and {@link ImageContext}. It's
   * possible to add {@link ImageContext} to each image to be annotated.
   */
  public static class AnnotateImagesFromGcsUriWithContext
      extends AnnotateImages<KV<String, ImageContext>> {

    public AnnotateImagesFromGcsUriWithContext(
        List<Feature> featureList, long batchSize, int desiredRequestParallelism) {
      super(featureList, batchSize, desiredRequestParallelism);
    }

    /**
     * Maps {@link KV} of {@link String} (GCS URI to the image) and {@link ImageContext} to a valid
     * {@link AnnotateImageRequest}.
     *
     * @param input Input element.
     * @param ctx optional image context, ignored here since the input holds context.
     * @return a valid request.
     */
    @Override
    public AnnotateImageRequest mapToRequest(
        KV<String, ImageContext> input, @Nullable ImageContext ctx) {
      ImageSource imageSource = ImageSource.newBuilder().setGcsImageUri(input.getKey()).build();
      Image image = Image.newBuilder().setSource(imageSource).build();
      AnnotateImageRequest.Builder builder =
          AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(featureList);
      if (input.getValue() != null) {
        builder.setImageContext(input.getValue());
      }
      return builder.build();
    }
  }

  /**
   * Accepts {@link KV}s of {@link ByteString} (encoded image contents) and {@link ImageContext}.
   * It's possible to add {@link ImageContext} to each image to be annotated.
   */
  public static class AnnotateImagesFromBytesWithContext
      extends AnnotateImages<KV<ByteString, ImageContext>> {

    public AnnotateImagesFromBytesWithContext(
        List<Feature> featureList, long batchSize, int desiredRequestParallelism) {
      super(featureList, batchSize, desiredRequestParallelism);
    }

    /**
     * Maps {@link KV} of {@link ByteString} (encoded image contents) and {@link ImageContext} to
     * {@link AnnotateImageRequest}.
     *
     * @param input Input element.
     * @param ctx optional image context.
     * @return valid request element.
     */
    @Override
    public AnnotateImageRequest mapToRequest(
        KV<ByteString, ImageContext> input, @Nullable ImageContext ctx) {
      Image image = Image.newBuilder().setContent(input.getKey()).build();
      AnnotateImageRequest.Builder builder =
          AnnotateImageRequest.newBuilder().setImage(image).addAllFeatures(featureList);
      if (input.getValue() != null) {
        builder.setImageContext(input.getValue());
      }
      return builder.build();
    }
  }
}
