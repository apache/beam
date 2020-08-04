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

import com.google.cloud.videointelligence.v1.Feature;
import com.google.cloud.videointelligence.v1.LabelDetectionConfig;
import com.google.cloud.videointelligence.v1.VideoAnnotationResults;
import com.google.cloud.videointelligence.v1.VideoContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VideoIntelligenceIT {
  @Rule public TestPipeline testPipeline = TestPipeline.create();
  private static final String VIDEO_URI =
      "gs://apache-beam-samples/advanced_analytics/video/gbikes_dinosaur.mp4";
  private List<Feature> featureList = Collections.singletonList(Feature.LABEL_DETECTION);

  @Test
  public void annotateVideoFromURIWithContext() {
    VideoContext context =
        VideoContext.newBuilder()
            .setLabelDetectionConfig(LabelDetectionConfig.newBuilder().setModel("builtin/latest"))
            .build();

    PCollection<List<VideoAnnotationResults>> annotationResults =
        testPipeline
            .apply(Create.of(KV.of(VIDEO_URI, context)))
            .apply("Annotate video", VideoIntelligence.annotateFromUriWithContext(featureList));
    PAssert.that(annotationResults).satisfies(new VerifyVideoAnnotationResult());
    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyVideoAnnotationResult
      implements SerializableFunction<Iterable<List<VideoAnnotationResults>>, Void> {

    @Override
    public Void apply(Iterable<List<VideoAnnotationResults>> input) {
      List<Boolean> labelEvaluations = new ArrayList<>();
      input.forEach(findStringMatchesInVideoAnnotationResultList(labelEvaluations, "bicycle"));
      assertEquals(Boolean.TRUE, labelEvaluations.contains(Boolean.TRUE));
      return null;
    }

    private Consumer<List<VideoAnnotationResults>> findStringMatchesInVideoAnnotationResultList(
        List<Boolean> labelEvaluations, String toMatch) {
      return videoAnnotationResults ->
          labelEvaluations.add(
              videoAnnotationResults.stream()
                  .anyMatch(result -> entityWithDescriptionFoundInSegmentLabels(toMatch, result)));
    }

    private boolean entityWithDescriptionFoundInSegmentLabels(
        String toMatch, VideoAnnotationResults result) {
      return result.getSegmentPresenceLabelAnnotationsList().stream()
          .anyMatch(
              labelAnnotation -> labelAnnotation.getEntity().getDescription().contains(toMatch));
    }
  }
}
