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

import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.Feature;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudVisionTest {

  private static final String TEST_URI = "test_uri";
  private static final ByteString TEST_BYTES = ByteString.copyFromUtf8("12345");;
  private List<Feature> features =
      Collections.singletonList(Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build());

  @Test
  public void shouldConvertStringToRequest() {
    CloudVision.AnnotateImagesFromGcsUri annotateImagesFromGcsUri =
        CloudVision.annotateImagesFromGcsUri(null, features, 1, 1);
    AnnotateImageRequest request = annotateImagesFromGcsUri.mapToRequest(TEST_URI, null);
    assertEquals(1, request.getFeaturesCount());
    assertEquals(TEST_URI, request.getImage().getSource().getGcsImageUri());
  }

  @Test
  public void shouldConvertByteStringToRequest() {
    CloudVision.AnnotateImagesFromBytes annotateImagesFromBytes =
        CloudVision.annotateImagesFromBytes(null, features, 1, 1);
    AnnotateImageRequest request = annotateImagesFromBytes.mapToRequest(TEST_BYTES, null);
    assertEquals(1, request.getFeaturesCount());
    assertEquals(TEST_BYTES, request.getImage().getContent());
  }

  @Test
  public void shouldConvertKVOfStringToRequest() {
    CloudVision.AnnotateImagesFromGcsUriWithContext annotateImagesFromGcsUriWithContext =
        CloudVision.annotateImagesFromGcsUriWithContext(features, 1, 1);
    AnnotateImageRequest request =
        annotateImagesFromGcsUriWithContext.mapToRequest(KV.of(TEST_URI, null), null);
    assertEquals(1, request.getFeaturesCount());
    assertEquals(TEST_URI, request.getImage().getSource().getGcsImageUri());
  }

  @Test
  public void shouldConvertKVOfBytesToRequest() {
    CloudVision.AnnotateImagesFromBytesWithContext annotateImagesFromBytesWithContext =
        CloudVision.annotateImagesFromBytesWithContext(features, 1, 1);
    AnnotateImageRequest request =
        annotateImagesFromBytesWithContext.mapToRequest(KV.of(TEST_BYTES, null), null);
    assertEquals(1, request.getFeaturesCount());
    assertEquals(TEST_BYTES, request.getImage().getContent());
  }
}
