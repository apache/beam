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
package org.apache.beam.sdk.ml.inference.gemini;

import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateImagesConfig;
import com.google.genai.types.GenerateImagesResponse;
import java.util.ArrayList;
import java.util.List;

/** Common inference functions for Gemini. */
public class GeminiInferenceFunctions {

  /** Generates content from string prompts using the standard generateContent API. */
  public static GeminiRequestFunction<GeminiStringInput, GeminiStringResponse>
      generateFromString() {
    return (modelName, batch, client) -> {
      List<GeminiStringResponse> results = new ArrayList<>();
      for (GeminiStringInput input : batch) {
        GenerateContentResponse response =
            client.models.generateContent(
                modelName, input.getText(), GenerateContentConfig.builder().build());
        String text = response.text();
        results.add(new GeminiStringResponse(text != null ? text : ""));
      }
      return results;
    };
  }

  /** Generates images from string prompts using the generateImages API. */
  public static GeminiRequestFunction<GeminiStringInput, GeminiImageResponse>
      generateImageFromString() {
    return (modelName, batch, client) -> {
      List<GeminiImageResponse> results = new ArrayList<>();
      for (GeminiStringInput input : batch) {
        GenerateImagesResponse response =
            client.models.generateImages(
                modelName, input.getText(), GenerateImagesConfig.builder().build());
        // Retrieve the base64 string or bytes from the first generated image
        List<com.google.genai.types.Image> images = response.images();
        if (images != null && !images.isEmpty()) {
          byte[] imageBytes = images.get(0).imageBytes().orElse(new byte[0]);
          results.add(new GeminiImageResponse(imageBytes));
        } else {
          results.add(new GeminiImageResponse(new byte[0]));
        }
      }
      return results;
    };
  }
}
