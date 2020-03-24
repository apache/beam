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
import com.google.cloud.videointelligence.v1.*;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public abstract class AnnotateVideo<T> extends DoFn<T, List<VideoAnnotationResults>> {

  protected final PCollectionView<Map<T, VideoContext>> contextSideInput;
  protected final List<Feature> featureList;
  VideoIntelligenceServiceClient videoIntelligenceServiceClient;

  public AnnotateVideo(
      PCollectionView<Map<T, VideoContext>> contextSideInput, List<Feature> featureList) {
    this.contextSideInput = contextSideInput;
    this.featureList = featureList;
  }

  public AnnotateVideo(List<Feature> featureList) {
    contextSideInput = null;
    this.featureList = featureList;
  }

  @StartBundle
  public void startBundle() throws IOException {
    videoIntelligenceServiceClient = VideoIntelligenceServiceClient.create();
  }

  @Teardown
  public void teardown() {
    videoIntelligenceServiceClient.close();
  }

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

  @ProcessElement
  public abstract void processElement(ProcessContext context)
      throws ExecutionException, InterruptedException;
}
