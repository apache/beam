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

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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

class Images {
  private static final TypeDescriptor<Struct> STRUCT_TYPE = TypeDescriptor.of(Struct.class);
  private static final String URL_FIELD = "url";
  static final StructCoder URL_STRUCT_CODER = StructCoder.of(URL_FIELD);

  static PCollection<KV<Struct, ImageRequest>> requestsOf(List<String> urls, Pipeline pipeline) {
    TypeDescriptor<KV<Struct, ImageRequest>> imageRequestType =
        TypeDescriptors.kvs(STRUCT_TYPE, ImageRequest.TYPE);

    Coder<KV<Struct, ImageRequest>> kvCoder = KvCoder.of(URL_STRUCT_CODER, ImageRequestCoder.of());

    return pipeline
        .apply("urls", Create.of(urls))
        .apply(
            ImageRequest.class.getSimpleName(),
            MapElements.into(imageRequestType)
                .via(url -> KV.of(buildLabel(url), ImageRequest.of(url))))
        .setCoder(kvCoder);
  }

  // [START webapis_get_images]

  static Result<KV<Struct, ImageResponse>> imagesOf(List<String> urls, Pipeline pipeline) {

    Coder<KV<Struct, ImageResponse>> kvCoder =
        KvCoder.of(URL_STRUCT_CODER, ImageResponseCoder.of());

    return requestsOf(urls, pipeline)
        .apply(
            ImageResponse.class.getSimpleName(),
            RequestResponseIO.of(HttpImageClient.of(), kvCoder));
  }

  // [END webapis_get_images]

  static PCollection<KV<Struct, String>> displayOf(PCollection<KV<Struct, ImageResponse>> result) {
    TypeDescriptor<KV<Struct, String>> dataUrlType =
        TypeDescriptors.kvs(STRUCT_TYPE, TypeDescriptors.strings());

    Coder<KV<Struct, String>> kvCoder = KvCoder.of(URL_STRUCT_CODER, StringUtf8Coder.of());

    return result
        .apply(
            "Display summary",
            MapElements.into(dataUrlType).via(kv -> KV.of(kv.getKey(), displayOf(kv.getValue()))))
        .setCoder(kvCoder);
  }

  private static String displayOf(ImageResponse response) {
    return String.format("mimeType=%s, size=%s", response.getMimeType(), response.getData().size());
  }

  private static Struct buildLabel(String url) {
    return Struct.newBuilder()
        .putFields(URL_FIELD, Value.newBuilder().setStringValue(url).build())
        .build();
  }
}
