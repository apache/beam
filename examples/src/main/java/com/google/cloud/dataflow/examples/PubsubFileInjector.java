/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.RateLimiting;
import com.google.cloud.dataflow.sdk.util.Transport;

import java.io.IOException;

/**
 * A batch Dataflow pipeline for injecting a set of GCS files into
 * a PubSub topic line by line.
 *
 * <p>  This is useful for testing streaming
 * pipelines. Note that since batch pipelines might retry chunks, this
 * does _not_ guarantee exactly-once injection of file data. Some lines may
 * be published multiple times.
 * </p>
 */
public class PubsubFileInjector {

  /** A DoFn that publishes lines to Google Cloud PubSub. */
  static class Publish extends DoFn<String, Void> {
    private String outputTopic;
    public transient Pubsub pubsub;

    Publish(String outputTopic) {
      this.outputTopic = outputTopic;
    }

    @Override
    public void startBundle(Context context) {
      StreamingOptions options =
          context.getPipelineOptions().as(StreamingOptions.class);
      this.pubsub = Transport.newPubsubClient(options).build();
    }

    @Override
    public void processElement(ProcessContext c) throws IOException {
      PubsubMessage pubsubMessage = new PubsubMessage();
      pubsubMessage.encodeData(c.element().getBytes());
      PublishRequest publishRequest = new PublishRequest();
      publishRequest.setTopic(outputTopic).setMessage(pubsubMessage);
      this.pubsub.topics().publish(publishRequest).execute();
    }
  }

  /**
   * Command line parameter options.
   */
  private interface PubsubFileInjectorOptions extends PipelineOptions {
    @Description("GCS location of files.")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Topic to publish on.")
    @Validation.Required
    String getOutputTopic();
    void setOutputTopic(String value);
  }

  /**
   * Sets up and starts streaming pipeline.
   */
  public static void main(String[] args) {
    PubsubFileInjectorOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(PubsubFileInjectorOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(TextIO.Read.from(options.getInput()))
        .apply(RateLimiting.perWorker(new Publish(options.getOutputTopic()))
            .withMaxParallelism(20));

    pipeline.run();
  }
}
