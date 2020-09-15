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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.videointelligence.v1.AnnotateVideoProgress;
import com.google.cloud.videointelligence.v1.AnnotateVideoRequest;
import com.google.cloud.videointelligence.v1.AnnotateVideoResponse;
import com.google.cloud.videointelligence.v1.Feature;
import com.google.cloud.videointelligence.v1.VideoAnnotationResults;
import com.google.cloud.videointelligence.v1.VideoContext;
import com.google.cloud.videointelligence.v1.VideoIntelligenceServiceClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Base class for DoFns used in VideoIntelligence transforms.
 *
 * @param <T> Class of input data being passed in - either ByteString - video data encoded into.
 *     String or String - a GCS URI of the video to be annotated.
 */
@Experimental
abstract class AnnotateVideoFn<T> extends DoFn<T, List<VideoAnnotationResults>> {

  protected final PCollectionView<Map<T, VideoContext>> contextSideInput;
  protected final List<Feature> featureList;
  VideoIntelligenceServiceClient videoIntelligenceServiceClient;

  public AnnotateVideoFn(
      PCollectionView<Map<T, VideoContext>> contextSideInput, List<Feature> featureList) {
    this.contextSideInput = contextSideInput;
    this.featureList = featureList;
  }

  public AnnotateVideoFn(List<Feature> featureList) {
    contextSideInput = null;
    this.featureList = featureList;
  }

  @Setup
  public void setup() throws IOException {
    videoIntelligenceServiceClient = VideoIntelligenceServiceClient.create();
  }

  @Teardown
  public void teardown() {
    videoIntelligenceServiceClient.close();
  }

  /**
   * Call the Video Intelligence Cloud AI service and return annotation results.
   *
   * @param elementURI This or elementContents is required. GCS address of video to be annotated
   * @param elementContents this or elementURI is required. Hex-encoded contents of video to be
   *     annotated
   * @param videoContext Optional context for video annotation.
   * @return
   */
  List<VideoAnnotationResults> getVideoAnnotationResults(
      String elementURI, ByteString elementContents, VideoContext videoContext)
      throws InterruptedException, ExecutionException {
    AnnotateVideoRequest.Builder requestBuilder =
        AnnotateVideoRequest.newBuilder().addAllFeatures(featureList);
    if (elementURI != null) {
      requestBuilder.setInputUri(elementURI);
    } else if (elementContents != null) {
      requestBuilder.setInputContent(elementContents);
    } else {
      throw new IllegalArgumentException("Either elementURI or elementContents should be non-null");
    }
    if (videoContext != null) {
      requestBuilder.setVideoContext(videoContext);
    }
    AnnotateVideoRequest annotateVideoRequest = requestBuilder.build();
    OperationFuture<AnnotateVideoResponse, AnnotateVideoProgress> annotateVideoAsync =
        videoIntelligenceServiceClient.annotateVideoAsync(annotateVideoRequest);
    return annotateVideoAsync.get().getAnnotationResultsList();
  }

  /** Process element implementation required. */
  @ProcessElement
  public abstract void processElement(ProcessContext context)
      throws ExecutionException, InterruptedException;
}
