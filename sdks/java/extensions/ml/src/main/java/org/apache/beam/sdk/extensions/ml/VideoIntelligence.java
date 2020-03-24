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

import com.google.cloud.videointelligence.v1.*;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

public class VideoIntelligence {

  public static AnnotateVideoFromURI annotateFromURI(
      List<Feature> featureList, PCollectionView<Map<String, VideoContext>> contextSideInput) {
    return new AnnotateVideoFromURI(contextSideInput, featureList);
  }

  public static AnnotateVideoFromBytes annotateFromBytes(
      PCollectionView<Map<ByteString, VideoContext>> contextSideInput, List<Feature> featureList) {
    return new AnnotateVideoFromBytes(contextSideInput, featureList);
  }

  public static AnnotateVideoURIWithContext annotateFromUriWithContext(List<Feature> featureList) {
    return new AnnotateVideoURIWithContext(featureList);
  }

  public static AnnotateVideoBytesWithContext annotateFromBytesWithContext(
      List<Feature> featureList) {
    return new AnnotateVideoBytesWithContext(featureList);
  }

  public static class AnnotateVideoFromURI extends AnnotateVideo<String> {

    public AnnotateVideoFromURI(
        PCollectionView<Map<String, VideoContext>> contextSideInput, List<Feature> featureList) {
      super(contextSideInput, featureList);
    }

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

  public static class AnnotateVideoFromBytes extends AnnotateVideo<ByteString> {

    public AnnotateVideoFromBytes(
        PCollectionView<Map<ByteString, VideoContext>> contextSideInput,
        List<Feature> featureList) {
      super(contextSideInput, featureList);
    }

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

  public static class AnnotateVideoURIWithContext extends AnnotateVideo<KV<String, VideoContext>> {

    public AnnotateVideoURIWithContext(List<Feature> featureList) {
      super(featureList);
    }

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

  public static class AnnotateVideoBytesWithContext
      extends AnnotateVideo<KV<ByteString, VideoContext>> {

    public AnnotateVideoBytesWithContext(List<Feature> featureList) {
      super(featureList);
    }

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
