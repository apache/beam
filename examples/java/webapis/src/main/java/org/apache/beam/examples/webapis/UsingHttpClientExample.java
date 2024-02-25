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
import java.util.List;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Example demonstrating reading from Web APIs GET endpoint using {@link RequestResponseIO}. */
class UsingHttpClientExample {

  // [START webapis_http_get]

  static void readFromGetEndpointExample(List<String> urls, Pipeline pipeline) {
    //        Pipeline pipeline = Pipeline.create();
    //        List<String> urls = ImmutableList.of(
    //                "https://storage.googleapis.com/generativeai-downloads/images/cake.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/chocolate.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/factory.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/scones.jpg"
    //        );

    PCollection<KV<Struct, ImageRequest>> requests = Images.requestsOf(urls, pipeline);

    KvCoder<Struct, ImageResponse> responseCoder =
        KvCoder.of(Images.URL_STRUCT_CODER, ImageResponseCoder.of());

    Result<KV<Struct, ImageResponse>> result =
        requests.apply(
            ImageResponse.class.getSimpleName(),
            RequestResponseIO.of(HttpImageClient.of(), responseCoder));

    result.getFailures().apply("logErrors", Log.errorOf());

    Images.displayOf(result.getResponses()).apply("logResponses", Log.infoOf());
  }

  // [END webapis_http_get]
}
