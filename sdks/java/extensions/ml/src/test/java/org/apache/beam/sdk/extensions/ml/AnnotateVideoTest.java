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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.videointelligence.v1.AnnotateVideoProgress;
import com.google.cloud.videointelligence.v1.AnnotateVideoResponse;
import com.google.cloud.videointelligence.v1.Feature;
import com.google.cloud.videointelligence.v1.VideoAnnotationResults;
import com.google.cloud.videointelligence.v1.VideoIntelligenceServiceClient;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AnnotateVideoTest {

  private static final String TEST_URI = "fake_uri";
  private static final ByteString TEST_BYTES = ByteString.copyFromUtf8("12345");

  @Mock private VideoIntelligenceServiceClient serviceClient;
  @Mock private OperationFuture<AnnotateVideoResponse, AnnotateVideoProgress> future;
  @Mock private AnnotateVideoResponse response;

  @Test
  public void shouldReturnAListOfAnnotations() throws ExecutionException, InterruptedException {
    when(response.getAnnotationResultsList())
        .thenReturn(Collections.singletonList(VideoAnnotationResults.newBuilder().build()));
    when(future.get()).thenReturn(response);
    when(serviceClient.annotateVideoAsync(any())).thenReturn(future);
    AnnotateVideoFromBytesFn annotateVideoFromBytesFn =
        new AnnotateVideoFromBytesFn(null, Collections.singletonList(Feature.LABEL_DETECTION));

    annotateVideoFromBytesFn.videoIntelligenceServiceClient = serviceClient;
    List<VideoAnnotationResults> videoAnnotationResults =
        annotateVideoFromBytesFn.getVideoAnnotationResults(TEST_URI, null, null);
    assertEquals(1, videoAnnotationResults.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowErrorWhenBothInputTypesNull()
      throws ExecutionException, InterruptedException {
    AnnotateVideoFromBytesFn annotateVideoFromBytesFn =
        new AnnotateVideoFromBytesFn(null, Collections.singletonList(Feature.LABEL_DETECTION));
    annotateVideoFromBytesFn.getVideoAnnotationResults(null, null, null);
  }
}
