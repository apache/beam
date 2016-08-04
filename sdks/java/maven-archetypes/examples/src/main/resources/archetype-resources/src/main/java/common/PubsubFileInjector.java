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
package ${package}.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.IntraBundleParallelization;
import org.apache.beam.sdk.util.Transport;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Arrays;

/**
 * A batch Dataflow pipeline for injecting a set of GCS files into
 * a PubSub topic line by line. Empty lines are skipped.
 *
 * <p>This is useful for testing streaming
 * pipelines. Note that since batch pipelines might retry chunks, this
 * does _not_ guarantee exactly-once injection of file data. Some lines may
 * be published multiple times.
 * </p>
 */
public class PubsubFileInjector {

  /**
   * An incomplete {@code PubsubFileInjector} transform with unbound output topic.
   */
  public static class Unbound {
    private final String timestampLabelKey;

    Unbound() {
      this.timestampLabelKey = null;
    }

    Unbound(String timestampLabelKey) {
      this.timestampLabelKey = timestampLabelKey;
    }

    Unbound withTimestampLabelKey(String timestampLabelKey) {
      return new Unbound(timestampLabelKey);
    }

    public Bound publish(String outputTopic) {
      return new Bound(outputTopic, timestampLabelKey);
    }
  }

  /** A DoFn that publishes non-empty lines to Google Cloud PubSub. */
  public static class Bound extends OldDoFn<String, Void> {
    private final String outputTopic;
    private final String timestampLabelKey;
    public transient Pubsub pubsub;

    public Bound(String outputTopic, String timestampLabelKey) {
      this.outputTopic = outputTopic;
      this.timestampLabelKey = timestampLabelKey;
    }

    @Override
    public void startBundle(Context context) {
      this.pubsub =
          Transport.newPubsubClient(context.getPipelineOptions().as(DataflowPipelineOptions.class))
              .build();
    }

    @Override
    public void processElement(ProcessContext c) throws IOException {
      if (c.element().isEmpty()) {
        return;
      }
      PubsubMessage pubsubMessage = new PubsubMessage();
      pubsubMessage.encodeData(c.element().getBytes());
      if (timestampLabelKey != null) {
        pubsubMessage.setAttributes(
            ImmutableMap.of(timestampLabelKey, Long.toString(c.timestamp().getMillis())));
      }
      PublishRequest publishRequest = new PublishRequest();
      publishRequest.setMessages(Arrays.asList(pubsubMessage));
      this.pubsub.projects().topics().publish(outputTopic, publishRequest).execute();
    }
  }

  /**
   * Creates a {@code PubsubFileInjector} transform with the given timestamp label key.
   */
  public static Unbound withTimestampLabelKey(String timestampLabelKey) {
    return new Unbound(timestampLabelKey);
  }

  /**
   * Creates a {@code PubsubFileInjector} transform that publishes to the given output topic.
   */
  public static Bound publish(String outputTopic) {
    return new Unbound().publish(outputTopic);
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
        .apply(IntraBundleParallelization.of(PubsubFileInjector.publish(options.getOutputTopic()))
            .withMaxParallelism(20));

    pipeline.run();
  }
}
