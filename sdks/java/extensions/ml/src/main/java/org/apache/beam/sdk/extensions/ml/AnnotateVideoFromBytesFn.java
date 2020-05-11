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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Implementation of AnnotateVideoFn accepting ByteStrings as contents of input PCollection. Videos
 * decoded from the ByteStrings are annotated.
 */
@Experimental
class AnnotateVideoFromBytesFn extends AnnotateVideoFn<ByteString> {

  public AnnotateVideoFromBytesFn(
      PCollectionView<Map<ByteString, VideoContext>> contextSideInput, List<Feature> featureList) {
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
