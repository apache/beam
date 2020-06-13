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

import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Feature;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudVisionIT {
  @Rule public TestPipeline testPipeline = TestPipeline.create();
  private static final String TEST_IMAGE_URI = "gs://cloud-samples-data/vision/label/setagaya.jpeg";
  private static final String EXPECTED_LABEL = "Street";
  private List<Feature> features =
      Collections.singletonList(Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build());
  private static final int NUMBER_OF_KEYS = 1;

  @Test
  public void annotateImageFromURINoContext() {
    PCollection<List<AnnotateImageResponse>> annotationResponses =
        testPipeline
            .apply(Create.of(TEST_IMAGE_URI))
            .apply(CloudVision.annotateImagesFromGcsUri(null, features, 1, NUMBER_OF_KEYS));

    PAssert.that(annotationResponses).satisfies(new VerifyImageAnnotationResult());

    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyImageAnnotationResult
      implements org.apache.beam.sdk.transforms.SerializableFunction<
          Iterable<List<AnnotateImageResponse>>, Void> {
    @Override
    public Void apply(Iterable<List<AnnotateImageResponse>> input) {
      List<Boolean> labelMatches = new ArrayList<>();
      input.forEach(findStringMatchesInAnnotationResponse(labelMatches, EXPECTED_LABEL));
      return null;
    }

    private Consumer<? super List<AnnotateImageResponse>> findStringMatchesInAnnotationResponse(
        List<Boolean> labelMatches, String expectedLabel) {
      return annotateImageResponses -> {
        labelMatches.add(
            annotateImageResponses.stream()
                .anyMatch(result -> entityWithDescriptionFoundInResult(expectedLabel, result)));
      };
    }

    private boolean entityWithDescriptionFoundInResult(
        String expectedLabel, AnnotateImageResponse result) {
      return result.getLabelAnnotationsList().stream()
          .anyMatch(labelAnnotation -> labelAnnotation.getDescription().equals(expectedLabel));
    }
  }
}
