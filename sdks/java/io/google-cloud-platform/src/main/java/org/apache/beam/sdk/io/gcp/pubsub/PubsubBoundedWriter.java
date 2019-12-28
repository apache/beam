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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.PubsubTopic;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Writer to Pubsub which batches messages from bounded collections. */
class PubsubBoundedWriter extends DoFn<PubsubMessage, Void> {
  /**
   * Max batch byte size. Messages are base64 encoded which encodes each set of three bytes into
   * four bytes.
   */
  private static final int MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT = ((10 * 1000 * 1000) / 4) * 3;

  private static final int MAX_PUBLISH_BATCH_SIZE = 100;

  private transient List<OutgoingMessage> output;
  private transient PubsubClient pubsubClient;
  private transient int currentOutputBytes;

  private final int maxPublishBatchByteSize;
  private final int maxPublishBatchSize;
  private final PubsubIO.Write<?> write;

  private PubsubBoundedWriter(PubsubIO.Write<?> write) {
    Preconditions.checkNotNull(write.getTopicProvider());
    this.maxPublishBatchSize =
        MoreObjects.firstNonNull(write.getMaxBatchSize(), MAX_PUBLISH_BATCH_SIZE);
    this.maxPublishBatchByteSize =
        MoreObjects.firstNonNull(write.getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT);
    this.write = write;
  }

  static <T> PubsubBoundedWriter of(PubsubIO.Write<T> write) {
    return new PubsubBoundedWriter(write);
  }

  @StartBundle
  public void startBundle(StartBundleContext c) throws IOException {
    this.output = new ArrayList<>();
    this.currentOutputBytes = 0;

    // NOTE: idAttribute is ignored.
    this.pubsubClient =
        write
            .getPubsubClientFactory()
            .newClient(
                write.getTimestampAttribute(),
                null,
                c.getPipelineOptions().as(PubsubOptions.class));
  }

  @ProcessElement
  public void processElement(@Element PubsubMessage message, ProcessContext c)
      throws IOException, SizeLimitExceededException {
    if (message.getData().size() > maxPublishBatchByteSize) {
      String msg =
          String.format(
              "Pub/Sub message size (%d) exceeded maximum batch size (%d)",
              message.getData().size(), maxPublishBatchByteSize);
      throw new SizeLimitExceededException(msg);
    }

    // Checking before adding the message stops us from violating the max bytes
    if (((currentOutputBytes + message.getData().size()) >= maxPublishBatchByteSize)
        || (output.size() >= maxPublishBatchSize)) {
      publish();
    }

    // NOTE: The record id is always null.
    output.add(OutgoingMessage.of(message, c.timestamp().getMillis(), null));
    currentOutputBytes += message.getData().size();
  }

  @FinishBundle
  public void finishBundle() throws IOException {
    if (!output.isEmpty()) {
      publish();
    }
    output = null;
    currentOutputBytes = 0;
    pubsubClient.close();
    pubsubClient = null;
  }

  private void publish() throws IOException {
    PubsubTopic topic = write.getTopicProvider().get();
    int n = pubsubClient.publish(PubsubClient.topicPathFromPath(topic.asPath()), output);
    checkState(n == output.size());
    output.clear();
    currentOutputBytes = 0;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.delegate(write);
  }
}
