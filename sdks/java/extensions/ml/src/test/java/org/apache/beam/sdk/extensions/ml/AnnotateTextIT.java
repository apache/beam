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

import com.google.cloud.language.v1.AnnotateTextRequest;
import com.google.cloud.language.v1.AnnotateTextResponse;
import com.google.cloud.language.v1.Document;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AnnotateTextIT {
  @Rule public TestPipeline testPipeline = TestPipeline.create();
  private static final String TEST_STRING = "Hello, world!";

  @Test
  public void analyzesLanguage() {
    Document doc =
        Document.newBuilder().setContent(TEST_STRING).setType(Document.Type.PLAIN_TEXT).build();
    AnnotateTextRequest.Features features =
        AnnotateTextRequest.Features.newBuilder().setExtractSyntax(true).build();
    PCollection<AnnotateTextResponse> responses =
        testPipeline
            .apply(Create.of(doc))
            .apply(AnnotateText.newBuilder().setFeatures(features).build());
    PAssert.that(responses).satisfies(new VerifyTextAnnotationResult());
    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyTextAnnotationResult
      implements SerializableFunction<Iterable<AnnotateTextResponse>, Void> {

    @Override
    public Void apply(Iterable<AnnotateTextResponse> input) {
      List<Boolean> labelEvaluations = new ArrayList<>();
      input.forEach(
          response -> labelEvaluations.add(response.getLanguage().equalsIgnoreCase("en")));
      assertEquals(Boolean.TRUE, labelEvaluations.contains(Boolean.TRUE));
      return null;
    }
  }
}
