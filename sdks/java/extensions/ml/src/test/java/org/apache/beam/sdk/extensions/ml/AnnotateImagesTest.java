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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AnnotateImagesTest {
  @Mock private ImageAnnotatorClient imageAnnotatorClient;
  @Mock private BatchAnnotateImagesResponse response;

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnLargeBatchSize() {
    CloudVision.annotateImagesFromBytes(null, null, 20, 0);
  }

  @Test
  public void shouldReturnAnnotationList() {
    when(response.getResponsesList())
        .thenReturn(Collections.singletonList(AnnotateImageResponse.newBuilder().build()));
    when(imageAnnotatorClient.batchAnnotateImages(anyList())).thenReturn(response);
    AnnotateImages.PerformImageAnnotation performImageAnnotation =
        new AnnotateImages.PerformImageAnnotation(imageAnnotatorClient);
    List<AnnotateImageResponse> responseList =
        performImageAnnotation.getResponse(Collections.emptyList());
    assertEquals(1, responseList.size());
  }
}
