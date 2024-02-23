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

// [START webapis_using_client_code]

import static org.apache.beam.examples.webapis.GeminiAIClient.MODEL_GEMINI_PRO_VISION;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.vertexai.api.Blob;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.Part;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.XYChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeminiAIExample {

  private static final Logger LOG = LoggerFactory.getLogger(GeminiAIExample.class);

  // [START webapis_gemini_id_image]
  public static void whatIsThisImage(List<String> urls, GeminiAIOptions options) {
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
            .setProjectId(options.getProjectId())
            .setLocation(options.getLocation())
            .setModelName(MODEL_GEMINI_PRO_VISION)
            .build();

    Pipeline pipeline = Pipeline.create(options);

    Result<ImageResponse> imageResults =
        pipeline
            .apply("createUrls", Create.of(urls))
            .apply(
                "buildRequests",
                MapElements.into(TypeDescriptor.of(ImageRequest.class)).via(ImageRequest::of))
            .apply(
                "executeRequests",
                RequestResponseIO.of(HttpImageClient.of(), ImageResponseCoder.of()));

    imageResults.getFailures().apply("imageGetErrors", logErrorOf());

    Result<GenerateContentResponse> result =
        imageResults
            .getResponses()
            .apply(
                "buildRequests",
                MapElements.into(TypeDescriptor.of(GenerateContentRequest.class))
                    .via(
                        imageResponse -> {
                          ImageResponse safeResponse = checkStateNotNull(imageResponse);
                          return GenerateContentRequest.newBuilder()
                              .addContents(
                                  Content.newBuilder()
                                      .setRole("USER")
                                      .addParts(Part.newBuilder().setText("What is this picture?"))
                                      .addParts(
                                          Part.newBuilder()
                                              .setInlineData(
                                                  Blob.newBuilder()
                                                      .setData(safeResponse.getData())
                                                      .setMimeType(safeResponse.getMimeType())
                                                      .build())
                                              .build())
                                      .build())
                              .build();
                        }))
            .apply(
                "Ask Gemini AI",
                RequestResponseIO.ofCallerAndSetupTeardown(
                    client, SerializableCoder.of(GenerateContentResponse.class)));

    result.getFailures().apply("logErrors", logErrorOf());

    result.getResponses().apply("logResponses", logInfoOf());

    pipeline.run();
  }

  // [END webapis_gemini_id_image]

  // [START webapis_gemini_stats]

  public static void identifyStats(GeminiAIOptions options) throws IOException {
    //        GeminiAIOptions options = PipelineOptionsFactory.create().as(GeminiAIOptions.class);
    //        options.setLocation("us-central1");
    //        options.setProjectId("your-google-cloud-project-id");

    Pipeline pipeline = Pipeline.create(options);

    NormalDistribution normalDistribution = new NormalDistribution(10.0, 1.0);

    List<Double> x = new ArrayList<>();
    List<Double> y = new ArrayList<>();

    for (double i = 0; i <= 20; i++) {
      x.add(i);
      y.add(normalDistribution.density(i));
    }
    XYChart chart = QuickChart.getChart("_", "_", "_", "_", x, y);
    byte[] data = BitmapEncoder.getBitmapBytes(chart, BitmapEncoder.BitmapFormat.PNG);
    pipeline
        .apply("data", Create.of(data))
        .apply("save", FileIO.<byte[]>write().to("/tmp/chart").via(new ImageSink()));

    //    GeminiAIClient client = GeminiAIClient.builder()
    //            .setLocation(options.getLocation())
    //            .setProjectId(options.getProjectId())
    //            .build();

    pipeline.run();
  }

  private static class ImageSink implements FileIO.Sink<byte[]> {

    private @MonotonicNonNull WritableByteChannel channel;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      this.channel = channel;
    }

    @Override
    public void write(byte[] element) throws IOException {
      WritableByteChannel safeChannel = checkStateNotNull(channel);
      checkState(safeChannel.isOpen());
      safeChannel.write(ByteBuffer.wrap(element));
    }

    @Override
    public void flush() throws IOException {}
  }

  // [END webapis_gemini_stats]

  private static <T> ParDo.SingleOutput<T, T> logErrorOf() {
    return ParDo.of(new LogErrorFn<>());
  }

  private static <T> ParDo.SingleOutput<T, T> logInfoOf() {
    return ParDo.of(new LogInfoFn<>());
  }

  private static class LogErrorFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver) {
      LOG.error("{}: {}", Instant.now(), element);
      receiver.output(element);
    }
  }

  private static class LogInfoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver) {
      LOG.info("{}: {}", Instant.now(), element);
      receiver.output(element);
    }
  }
}

// [END webapis_using_client_code]
