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
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.KV;

/**
 * Implementation of AnnotateVideoFn accepting KVs as contents of input PCollection. Keys are the
 * ByteString encoded video contents, values - VideoContext objects.
 */
@Experimental
class AnnotateVideoBytesWithContextFn extends AnnotateVideoFn<KV<ByteString, VideoContext>> {

  public AnnotateVideoBytesWithContextFn(List<Feature> featureList) {
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
