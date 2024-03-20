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

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import java.util.List;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Various helper methods to handle image data. */
class Images {
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  /**
   * Converts a raw image URL list into an {@link ImageRequest} {@link PCollection}. It assigns the
   * {@link KV#getKey} to the original raw URL. This is to forward the URL along the pipeline for
   * comparing results to the original reference.
   */
  static PCollection<KV<String, ImageRequest>> requestsOf(List<String> urls, Pipeline pipeline) {
    TypeDescriptor<KV<String, ImageRequest>> imageRequestType =
        TypeDescriptors.kvs(strings(), ImageRequest.TYPE);

    Coder<KV<String, ImageRequest>> kvCoder = KvCoder.of(STRING_CODER, ImageRequestCoder.of());

    return pipeline
        .apply("urls", Create.of(urls))
        .apply(
            ImageRequest.class.getSimpleName(),
            MapElements.into(imageRequestType).via(url -> KV.of(url, ImageRequest.of(url))))
        .setCoder(kvCoder);
  }

  // [START webapis_java_get_images]

  /**
   * Processes a list of raw image URLs into a {@link ImageResponse} {@link PCollection} using
   * {@link RequestResponseIO}. The resulting {@link KV#getKey} is the original image URL.
   */
  static Result<KV<String, ImageResponse>> imagesOf(List<String> urls, Pipeline pipeline) {

    Coder<KV<String, ImageResponse>> kvCoder = KvCoder.of(STRING_CODER, ImageResponseCoder.of());

    return requestsOf(urls, pipeline)
        .apply(
            ImageResponse.class.getSimpleName(),
            RequestResponseIO.of(HttpImageClient.of(), kvCoder));
  }

  // [END webapis_java_get_images]

  /**
   * Converts a {@link KV} {@link ImageResponse} {@link PCollection} into a string {@link
   * PCollection} for convenient display of the results.
   */
  static PCollection<KV<String, String>> displayOf(PCollection<KV<String, ImageResponse>> result) {
    TypeDescriptor<KV<String, String>> displayType = TypeDescriptors.kvs(strings(), strings());

    Coder<KV<String, String>> kvCoder = KvCoder.of(STRING_CODER, STRING_CODER);

    return result
        .apply(
            "Display summary",
            MapElements.into(displayType).via(kv -> KV.of(kv.getKey(), displayOf(kv.getValue()))))
        .setCoder(kvCoder);
  }

  private static String displayOf(ImageResponse response) {
    return String.format("mimeType=%s, size=%s", response.getMimeType(), response.getData().size());
  }
}
