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

import com.google.cloud.videointelligence.v1.Feature;
import com.google.cloud.videointelligence.v1.VideoAnnotationResults;
import com.google.cloud.videointelligence.v1.VideoContext;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Factory class for PTransforms integrating with Google Cloud AI - VideoIntelligence service.
 * Converts GCS URIs of videos or ByteStrings with video contents into Lists of
 * VideoAnnotationResults.
 *
 * <p>Adding a side input of Maps of elements to VideoContext objects is allowed, so is using KVs of
 * element and VideoContext as input.
 *
 * <p>Service account with proper permissions is required to use these transforms.
 */
@Experimental
public class VideoIntelligence {

  /**
   * Annotates videos from GCS URIs.
   *
   * @param featureList List of features to be annotated
   * @param contextSideInput Optional side input with map of contexts to URIs
   * @return PTransform performing the necessary operations
   */
  public static AnnotateVideoFromUri annotateFromURI(
      List<Feature> featureList, PCollectionView<Map<String, VideoContext>> contextSideInput) {
    return new AnnotateVideoFromUri(contextSideInput, featureList);
  }

  /**
   * Annotates videos from ByteStrings of their contents.
   *
   * @param featureList List of features to be annotated
   * @param contextSideInput Optional side input with map of contexts to ByteStrings
   * @return PTransform performing the necessary operations
   */
  public static AnnotateVideoFromBytes annotateFromBytes(
      PCollectionView<Map<ByteString, VideoContext>> contextSideInput, List<Feature> featureList) {
    return new AnnotateVideoFromBytes(contextSideInput, featureList);
  }

  /**
   * Annotates videos from key-value pairs of GCS URI and VideoContext.
   *
   * @param featureList List of features to be annotated
   * @return PTransform performing the necessary operations
   */
  public static AnnotateVideoFromURIWithContext annotateFromUriWithContext(
      List<Feature> featureList) {
    return new AnnotateVideoFromURIWithContext(featureList);
  }

  /**
   * Annotates videos from key-value pairs of ByteStrings and VideoContext.
   *
   * @param featureList List of features to be annotated
   * @return PTransform performing the necessary operations
   */
  public static AnnotateVideoFromBytesWithContext annotateFromBytesWithContext(
      List<Feature> featureList) {
    return new AnnotateVideoFromBytesWithContext(featureList);
  }

  /**
   * A PTransform taking a PCollection of {@link String} and an optional side input with a context
   * map and emitting lists of {@link VideoAnnotationResults} for each element. Calls Cloud AI
   * VideoIntelligence.
   */
  @Experimental
  public static class AnnotateVideoFromUri
      extends PTransform<PCollection<String>, PCollection<List<VideoAnnotationResults>>> {

    private final PCollectionView<Map<String, VideoContext>> contextSideInput;
    private final List<Feature> featureList;

    protected AnnotateVideoFromUri(
        PCollectionView<Map<String, VideoContext>> contextSideInput, List<Feature> featureList) {
      this.contextSideInput = contextSideInput;
      this.featureList = featureList;
    }

    @Override
    public PCollection<List<VideoAnnotationResults>> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new AnnotateVideoFromURIFn(contextSideInput, featureList)));
    }
  }

  /**
   * A PTransform taking a PCollection of {@link ByteString} and an optional side input with a
   * context map and emitting lists of {@link VideoAnnotationResults} for each element. Calls Cloud
   * AI VideoIntelligence.
   */
  @Experimental
  public static class AnnotateVideoFromBytes
      extends PTransform<PCollection<ByteString>, PCollection<List<VideoAnnotationResults>>> {

    private final PCollectionView<Map<ByteString, VideoContext>> contextSideInput;
    private final List<Feature> featureList;

    protected AnnotateVideoFromBytes(
        PCollectionView<Map<ByteString, VideoContext>> contextSideInput,
        List<Feature> featureList) {
      this.contextSideInput = contextSideInput;
      this.featureList = featureList;
    }

    @Override
    public PCollection<List<VideoAnnotationResults>> expand(PCollection<ByteString> input) {
      return input.apply(ParDo.of(new AnnotateVideoFromBytesFn(contextSideInput, featureList)));
    }
  }

  /**
   * A PTransform taking a PCollection of {@link KV} of {@link String} and {@link VideoContext} and
   * emitting lists of {@link VideoAnnotationResults} for each element. Calls Cloud AI
   * VideoIntelligence.
   */
  @Experimental
  public static class AnnotateVideoFromURIWithContext
      extends PTransform<
          PCollection<KV<String, VideoContext>>, PCollection<List<VideoAnnotationResults>>> {

    private final List<Feature> featureList;

    protected AnnotateVideoFromURIWithContext(List<Feature> featureList) {
      this.featureList = featureList;
    }

    @Override
    public PCollection<List<VideoAnnotationResults>> expand(
        PCollection<KV<String, VideoContext>> input) {
      return input.apply(ParDo.of(new AnnotateVideoURIWithContextFn(featureList)));
    }
  }

  /**
   * A PTransform taking a PCollection of {@link KV} of {@link ByteString} and {@link VideoContext}
   * and emitting lists of {@link VideoAnnotationResults} for each element. Calls Cloud AI
   * VideoIntelligence.
   */
  @Experimental
  public static class AnnotateVideoFromBytesWithContext
      extends PTransform<
          PCollection<KV<ByteString, VideoContext>>, PCollection<List<VideoAnnotationResults>>> {

    private final List<Feature> featureList;

    protected AnnotateVideoFromBytesWithContext(List<Feature> featureList) {
      this.featureList = featureList;
    }

    @Override
    public PCollection<List<VideoAnnotationResults>> expand(
        PCollection<KV<ByteString, VideoContext>> input) {
      return input.apply(ParDo.of(new AnnotateVideoBytesWithContextFn(featureList)));
    }
  }
}
