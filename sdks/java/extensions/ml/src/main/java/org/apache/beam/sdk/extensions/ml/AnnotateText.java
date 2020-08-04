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

import com.google.auto.value.AutoValue;
import com.google.cloud.language.v1.AnnotateTextRequest;
import com.google.cloud.language.v1.AnnotateTextResponse;
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.LanguageServiceClient;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} using the Cloud AI Natural language processing capability. Takes an input
 * {@link PCollection} of {@link Document}s and converts them to {@link AnnotateTextResponse}s.
 *
 * <p>It is possible to provide a language hint to the API. A {@link
 * com.google.cloud.language.v1.AnnotateTextRequest.Features} object is required to configure
 * analysis types to be done on the data.
 */
@Experimental
@AutoValue
public abstract class AnnotateText
    extends PTransform<PCollection<Document>, PCollection<AnnotateTextResponse>> {

  public abstract @Nullable String languageHint();

  public abstract AnnotateTextRequest.Features features();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLanguageHint(String hint);

    public abstract Builder setFeatures(AnnotateTextRequest.Features features);

    public abstract AnnotateText build();
  }

  public static Builder newBuilder() {
    return new AutoValue_AnnotateText.Builder();
  }

  @Override
  public PCollection<AnnotateTextResponse> expand(PCollection<Document> input) {
    return input.apply(ParDo.of(new CallLanguageApi(languageHint(), features())));
  }

  private static class CallLanguageApi extends DoFn<Document, AnnotateTextResponse> {
    private final String languageHint;
    private final AnnotateTextRequest.Features features;

    private CallLanguageApi(String languageHint, AnnotateTextRequest.Features features) {
      this.languageHint = languageHint;
      this.features = features;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      try (LanguageServiceClient client = LanguageServiceClient.create()) {
        Document element = context.element();
        if (languageHint != null) {
          element = element.toBuilder().setLanguage(languageHint).build();
        }
        AnnotateTextResponse output = client.annotateText(element, features);
        context.output(output);
      }
    }
  }
}
