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

import java.util.List;
import org.apache.beam.io.requestresponse.ApiIOError;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Example demonstrating reading from Web APIs GET endpoint using {@link RequestResponseIO}. */
public class UsingHttpClientExample {

  // [START webapis_read_from_get_endpoint]

  public static Result<ImageResponse> readFromGetEndpointExample(
      List<String> urls, Pipeline pipeline) {
    //        Pipeline pipeline = Pipeline.create();
    //        List<String> urls = ImmutableList.of(
    //                "https://storage.googleapis.com/generativeai-downloads/images/cake.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/chocolate.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg",
    //                "https://storage.googleapis.com/generativeai-downloads/images/factory.png",
    //                "https://storage.googleapis.com/generativeai-downloads/images/scones.jpg"
    //        );

    return pipeline
        .apply("createUrls", Create.of(urls))
        .apply(
            "buildRequests",
            MapElements.into(TypeDescriptor.of(ImageRequest.class)).via(ImageRequest::of))
        .apply(
            "executeRequests", RequestResponseIO.of(HttpImageClient.of(), ImageResponseCoder.of()));
  }
  // [END webapis_read_from_get_endpoint]

  // [START webapis_handle_result]

  public static void handleResult(Result<ImageResponse> result) {
    result
        .getResponses()
        .apply(
            "responses",
            ParDo.of(
                new DoFn<ImageResponse, Void>() {
                  @ProcessElement
                  public void process(@Element ImageResponse response) {
                    // Do something with response
                  }
                }));

    result
        .getFailures()
        .apply(
            "errors",
            ParDo.of(
                new DoFn<ApiIOError, Void>() {
                  @ProcessElement
                  public void process(@Element ApiIOError error) {
                    // Do something with error
                  }
                }));
  }

  // [END webapis_handle_result]
}
