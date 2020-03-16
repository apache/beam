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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn to fetch a message from an Google Cloud Healthcare HL7v2 store based on msgID
 *
 * <p>This DoFn consumes a {@link PCollection<String>} of notifications from the HL7v2 store, and
 * fetches the actual {@link Message} object based on the id in the notification and will output a
 * {@link PCollectionTuple} which contains the output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 * <ul>
 *   <li>{@link FetchHL7v2Message#OUT} - Contains all {@link FailsafeElement} records successfully
 *       read from the HL7v2 store.
 *   <li>{@link FetchHL7v2Message#DEAD_LETTER} - Contains all {@link FailsafeElement} records which
 *       failed to be fetched from the HL7v2 store, with error message and stacktrace.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline pipeline = Pipeline.create(options)
 *
 * PCollection<String> msgIDs = pipeline.apply(
 *    "ReadHL7v2Notifications",
 *    PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));
 *
 * PCollectionTuple fetchResults = msgIDs.apply(
 *    "FetchHL7v2Messages",
 *    new FetchHL7v2Message;
 *
 * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
 * fetchResults.get(PubsubNotificationToHL7v2Message.DEAD_LETTER)
 *    .apply("WriteToDeadLetterQueue", ...);
 *
 * PCollection<Message> fetchedMessages = fetchResults.get(PubsubNotificationToHL7v2Message.OUT)
 *    .apply("ExtractFetchedMessage",
 *    MapElements
 *        .into(TypeDescriptor.of(Message.class))
 *        .via(FailsafeElement::getPayload));
 *
 * // Go about your happy path transformations.
 * fetchedMessages.apply("ProcessFetchedMessages", ...)
 *
 * }***
 * </pre>
 */
public class FetchHL7v2Message extends PTransform<PCollection<String>, PCollectionTuple> {
  // TODO: this should migrate to use the batch API once available

  /** The tag for the main output of HL7v2 Messages. */
  public static final TupleTag<FailsafeElement<String, Message>> OUT =
      new TupleTag<FailsafeElement<String, Message>>() {};
  /** The tag for the deadletter output of HL7v2 Messages. */
  public static final TupleTag<FailsafeElement<String, Message>> DEAD_LETTER =
      new TupleTag<FailsafeElement<String, Message>>() {};

  private static final Logger LOG = LoggerFactory.getLogger(FetchHL7v2Message.class);

  /** Instantiates a new Fetch HL7v2 message DoFn. */
  public FetchHL7v2Message() {}

  @Override
  public PCollectionTuple expand(PCollection<String> msgIds) {
    return msgIds.apply(
        ParDo.of(new HL7v2MessageGetFn()).withOutputTags(OUT, TupleTagList.of(DEAD_LETTER)));
  }
}
