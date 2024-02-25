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

import com.google.cloud.vertexai.api.Blob;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.Part;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
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

@SuppressWarnings({"unused"})
class GeminiAIExample {

//  [START webapis_identify_image]

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

    GeminiAIClient client =
        GeminiAIClient.builder()
            .setProjectId(options.getProject())
            .setLocation(options.getLocation())
            .setModelName(MODEL_GEMINI_PRO_VISION)
            .build();

    Pipeline pipeline = Pipeline.create(options);

    Result<KV<Struct, ImageResponse>> getImagesResult = Images.imagesOf(urls, pipeline);

    getImagesResult.getFailures().apply("Log get images errors", Log.errorOf());

    PCollection<KV<Struct, GenerateContentRequest>> requests =
        buildAIRequests("Identify Image", "What is this picture?", getImagesResult.getResponses());

    Result<KV<Struct, GenerateContentResponse>> responses = askAI(client, requests);

    responses.getFailures().apply("Log AI errors", Log.errorOf());

    responses.getResponses().apply("Log AI answers", Log.infoOf());

    pipeline.run();
  }

  //  [END webapis_identify_image]

  private static Result<KV<Struct, GenerateContentResponse>> askAI(
      GeminiAIClient client, PCollection<KV<Struct, GenerateContentRequest>> requestKV) {

    KvCoder<Struct, GenerateContentRequest> requestCoder =
        (KvCoder<Struct, GenerateContentRequest>) requestKV.getCoder();

    StructCoder keyCoder = (StructCoder) requestCoder.getKeyCoder();

    KvCoder<Struct, GenerateContentResponse> responseCoder =
        KvCoder.of(keyCoder, GenerateContentResponseCoder.of());

    // [START webapis_ask_ai]

//    PCollection<KV<Struct, GenerateContentRequest>> requestKV = ...
//    GeminiAIClient client =
//            GeminiAIClient.builder()
//                    .setProjectId(options.getProject())
//                    .setLocation(options.getLocation())
//                    .setModelName(MODEL_GEMINI_PRO_VISION)
//                    .build();

    return requestKV.apply(
        "Ask Gemini AI", RequestResponseIO.ofCallerAndSetupTeardown(client, responseCoder));

    // [END webapis_ask_ai]
  }



  private static PCollection<KV<Struct, GenerateContentRequest>> buildAIRequests(
      String stepName, String prompt, PCollection<KV<Struct, ImageResponse>> imagesKV) {

    KvCoder<Struct, ImageResponse> imagesKVCoder =
        (KvCoder<Struct, ImageResponse>) imagesKV.getCoder();

    StructCoder keyCoder = (StructCoder) imagesKVCoder.getKeyCoder();

    Coder<KV<Struct, GenerateContentRequest>> kvCoder =
        KvCoder.of(keyCoder, GenerateContentRequestCoder.of());

    TypeDescriptor<Struct> structType = TypeDescriptor.of(Struct.class);
    TypeDescriptor<GenerateContentRequest> requestType =
        TypeDescriptor.of(GenerateContentRequest.class);

    TypeDescriptor<KV<Struct, GenerateContentRequest>> requestKVType =
        TypeDescriptors.kvs(structType, requestType);

    // [START webapis_build_ai_requests]

    // PCollection<KV<Struct, ImageResponse>> imagesKV = ...

    return imagesKV
        .apply(
            stepName,
            MapElements.into(requestKVType)
                .via(
                    kv -> {
                      Struct key = kv.getKey();
                      ImageResponse safeResponse = checkStateNotNull(kv.getValue());
                      return buildAIRequest(
                          key, prompt, safeResponse.getData(), safeResponse.getMimeType());
                    }))
        .setCoder(kvCoder);

    // [END webapis_build_ai_requests]
  }

  private static KV<Struct, GenerateContentRequest> buildAIRequest(
      Struct key, String prompt, ByteString data, String mimeType) {
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
