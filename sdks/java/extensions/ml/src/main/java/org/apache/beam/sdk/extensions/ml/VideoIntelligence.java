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
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Factory class for AnnotateVideo subclasses. allows integration with Google Cloud AI -
 * VideoIntelligence service. Converts GCS URIs of videos or ByteStrings with video contents into
 * Lists of VideoAnnotationResults.
 *
 * <p>Adding a side input of Maps of elements to VideoContext objects is allowed, so is using KVs of
 * element and VideoContext as input.
 *
 * <p>Service account with proper permissions is required to use these transforms.
 */
public class VideoIntelligence {

  /**
   * Annotates videos from GCS URIs.
   *
   * @param featureList List of features to be annotated
   * @param contextSideInput Optional side input with map of contexts to URIs
   * @return DoFn performing the necessary operations
   */
  public static AnnotateVideoFromURI annotateFromURI(
      List<Feature> featureList, PCollectionView<Map<String, VideoContext>> contextSideInput) {
    return new AnnotateVideoFromURI(contextSideInput, featureList);
  }

  /**
   * Annotates videos from ByteStrings of their contents.
   *
   * @param featureList List of features to be annotated
   * @param contextSideInput Optional side input with map of contexts to ByteStrings
   * @return DoFn performing the necessary operations
   */
  public static AnnotateVideoFromBytes annotateFromBytes(
      PCollectionView<Map<ByteString, VideoContext>> contextSideInput, List<Feature> featureList) {
    return new AnnotateVideoFromBytes(contextSideInput, featureList);
  }

  /**
   * Annotates videos from key-value pairs of GCS URI and VideoContext.
   *
   * @param featureList List of features to be annotated
   * @return DoFn performing the necessary operations
   */
  public static AnnotateVideoURIWithContext annotateFromUriWithContext(List<Feature> featureList) {
    return new AnnotateVideoURIWithContext(featureList);
  }

  /**
   * Annotates videos from key-value pairs of ByteStrings and VideoContext.
   *
   * @param featureList List of features to be annotated
   * @return DoFn performing the necessary operations
   */
  public static AnnotateVideoBytesWithContext annotateFromBytesWithContext(
      List<Feature> featureList) {
    return new AnnotateVideoBytesWithContext(featureList);
  }

  /**
   * Implementation of AnnotateVideo accepting Strings as contents of input PCollection. Annotates
   * videos found on GCS based on URIs from input PCollection.
   */
  public static class AnnotateVideoFromURI extends AnnotateVideo<String> {

    public AnnotateVideoFromURI(
        PCollectionView<Map<String, VideoContext>> contextSideInput, List<Feature> featureList) {
      super(contextSideInput, featureList);
    }

    /** ProcessElement implementation. */
    @Override
    public void processElement(ProcessContext context)
        throws ExecutionException, InterruptedException {
      String elementURI = context.element();
      VideoContext videoContext = null;
      if (contextSideInput != null) {
        videoContext = context.sideInput(contextSideInput).get(elementURI);
      }
      List<VideoAnnotationResults> annotationResultsList =
          getVideoAnnotationResults(elementURI, null, videoContext);
      context.output(annotationResultsList);
    }
  }

  /**
   * Implementation of AnnotateVideo accepting ByteStrings as contents of input PCollection. Videos
   * decoded from the ByteStrings are annotated.
   */
  public static class AnnotateVideoFromBytes extends AnnotateVideo<ByteString> {

    public AnnotateVideoFromBytes(
        PCollectionView<Map<ByteString, VideoContext>> contextSideInput,
        List<Feature> featureList) {
      super(contextSideInput, featureList);
    }

    /** Implementation of ProcessElement. */
    @Override
    public void processElement(ProcessContext context)
        throws ExecutionException, InterruptedException {
      ByteString element = context.element();
      VideoContext videoContext = null;
      if (contextSideInput != null) {
        videoContext = context.sideInput(contextSideInput).get(element);
      }
      List<VideoAnnotationResults> videoAnnotationResults =
          getVideoAnnotationResults(null, element, videoContext);
      context.output(videoAnnotationResults);
    }
  }

  /**
   * Implementation of AnnotateVideo accepting KVs as contents of input PCollection. Keys are the
   * GCS URIs, values - VideoContext objects.
   */
  public static class AnnotateVideoURIWithContext extends AnnotateVideo<KV<String, VideoContext>> {

    public AnnotateVideoURIWithContext(List<Feature> featureList) {
      super(featureList);
    }

    /** ProcessElement implementation. */
    @Override
    public void processElement(ProcessContext context)
        throws ExecutionException, InterruptedException {
      String elementURI = context.element().getKey();
      VideoContext videoContext = context.element().getValue();
      List<VideoAnnotationResults> videoAnnotationResults =
          getVideoAnnotationResults(elementURI, null, videoContext);
      context.output(videoAnnotationResults);
    }
  }

  /**
   * Implementation of AnnotateVideo accepting KVs as contents of input PCollection. Keys are the
   * ByteString encoded video contents, values - VideoContext objects.
   */
  public static class AnnotateVideoBytesWithContext
      extends AnnotateVideo<KV<ByteString, VideoContext>> {

    public AnnotateVideoBytesWithContext(List<Feature> featureList) {
      super(featureList);
    }

    /** ProcessElement implementation. */
    @Override
    public void processElement(ProcessContext context)
        throws ExecutionException, InterruptedException {
      ByteString element = context.element().getKey();
      VideoContext videoContext = context.element().getValue();
      List<VideoAnnotationResults> videoAnnotationResults =
          getVideoAnnotationResults(null, element, videoContext);
      context.output(videoAnnotationResults);
    }
  }
}
