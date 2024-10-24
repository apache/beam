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
package org.apache.beam.examples.webapis;

import static org.apache.beam.examples.webapis.GeminiAIClient.MODEL_GEMINI_PRO_VISION;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.cloud.vertexai.api.Blob;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.Part;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Example using {@link RequestResponseIO} to process Gemini AI {@link GenerateContentRequest}s into
 * {@link GenerateContentResponse}s.
 */
class GeminiAIExample {

  //  [START webapis_java_identify_image]

  /** Demonstrates using Gemini AI to identify a images, acquired from their URLs. */
  static void whatIsThisImage(List<String> urls, GeminiAIOptions options) {
    //        GeminiAIOptions options = PipelineOptionsFactory.create().as(GeminiAIOptions.class);
    //        options.setLocation("us-central1");
    //        options.setProjectId("your-google-cloud-project-id");
    //
    //
    //        List<String> urls = ImmutableList.of(
    //                "https://storage.googleapis.com/generativeai-downloads/images/cake.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/chocolate.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/factory.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/scones.jpg"
    //        );

    // Step 1: Instantiate GeminiAIClient, the Caller and SetupTeardown implementation.
    GeminiAIClient client =
        GeminiAIClient.builder()
            .setProjectId(options.getProject())
            .setLocation(options.getLocation())
            .setModelName(MODEL_GEMINI_PRO_VISION)
            .build();

    Pipeline pipeline = Pipeline.create(options);

    // Step 2: Download the images from the list of urls.
    Result<KV<String, ImageResponse>> getImagesResult = Images.imagesOf(urls, pipeline);

    // Step 3: Log any image download errors.
    getImagesResult.getFailures().apply("Log get images errors", Log.errorOf());

    // Step 4: Build Gemini AI requests from the download image data with the prompt 'What is this
    // picture?'.
    PCollection<KV<String, GenerateContentRequest>> requests =
        buildAIRequests("Identify Image", "What is this picture?", getImagesResult.getResponses());

    // Step 5: Using RequestResponseIO, ask Gemini AI 'What is this picture?' for each downloaded
    // image.
    Result<KV<String, GenerateContentResponse>> responses = askAI(client, requests);

    // Step 6: Log any Gemini AI errors.
    responses.getFailures().apply("Log AI errors", Log.errorOf());

    // Step 7: Log the result of Gemini AI's image recognition.
    responses.getResponses().apply("Log AI answers", Log.infoOf());

    pipeline.run();
  }

  //  [END webapis_java_identify_image]

  private static Result<KV<String, GenerateContentResponse>> askAI(
      GeminiAIClient client, PCollection<KV<String, GenerateContentRequest>> requestKV) {

    KvCoder<String, GenerateContentRequest> requestCoder =
        (KvCoder<String, GenerateContentRequest>) requestKV.getCoder();

    Coder<String> keyCoder = requestCoder.getKeyCoder();

    KvCoder<String, GenerateContentResponse> responseCoder =
        KvCoder.of(keyCoder, GenerateContentResponseCoder.of());

    // [START webapis_java_ask_ai]

    //    PCollection<KV<Struct, GenerateContentRequest>> requestKV = ...
    //    GeminiAIClient client =
    //            GeminiAIClient.builder()
    //                    .setProjectId(options.getProject())
    //                    .setLocation(options.getLocation())
    //                    .setModelName(MODEL_GEMINI_PRO_VISION)
    //                    .build();

    return requestKV.apply(
        "Ask Gemini AI", RequestResponseIO.ofCallerAndSetupTeardown(client, responseCoder));

    // [END webapis_java_ask_ai]
  }

  private static PCollection<KV<String, GenerateContentRequest>> buildAIRequests(
      String stepName, String prompt, PCollection<KV<String, ImageResponse>> imagesKV) {

    KvCoder<String, ImageResponse> imagesKVCoder =
        (KvCoder<String, ImageResponse>) imagesKV.getCoder();

    Coder<String> keyCoder = imagesKVCoder.getKeyCoder();

    Coder<KV<String, GenerateContentRequest>> kvCoder =
        KvCoder.of(keyCoder, GenerateContentRequestCoder.of());

    TypeDescriptor<GenerateContentRequest> requestType =
        TypeDescriptor.of(GenerateContentRequest.class);

    TypeDescriptor<KV<String, GenerateContentRequest>> requestKVType =
        TypeDescriptors.kvs(strings(), requestType);

    // [START webapis_java_build_ai_requests]

    // PCollection<KV<Struct, ImageResponse>> imagesKV = ...

    return imagesKV
        .apply(
            stepName,
            MapElements.into(requestKVType)
                .via(
                    kv -> {
                      String key = kv.getKey();
                      ImageResponse safeResponse = checkStateNotNull(kv.getValue());
                      ByteString data = safeResponse.getData();
                      return buildAIRequest(key, prompt, data, safeResponse.getMimeType());
                    }))
        .setCoder(kvCoder);

    // [END webapis_java_build_ai_requests]
  }

  private static KV<String, GenerateContentRequest> buildAIRequest(
      String key, String prompt, ByteString data, String mimeType) {
    return KV.of(
        key,
        GenerateContentRequest.newBuilder()
            .addContents(
                Content.newBuilder()
                    .setRole("USER")
                    .addParts(Part.newBuilder().setText(prompt))
                    .addParts(
                        Part.newBuilder()
                            .setInlineData(
                                Blob.newBuilder().setData(data).setMimeType(mimeType).build())
                            .build())
                    .build())
            .build());
  }
}
