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

package org.apache.beam.sdk.nexmark.sources.pubsub;

import java.util.HashMap;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.nexmark.NexmarkLauncher;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.slf4j.LoggerFactory;

/**
 * Transform to convert Events to and from Pubsub messages.
 */
class MessageTransforms {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkLauncher.class);

  static ParDo.SingleOutput<PubsubMessage, Event> messageToEvent() {
    return ParDo.of(new DoFn<PubsubMessage, Event>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        byte[] payload = c.element().getPayload();
        try {
          Event event = CoderUtils.decodeFromByteArray(Event.CODER, payload);
          c.output(event);
        } catch (CoderException e) {
          LOG.error("Error while decoding Event from pusbSub message", e);
        }
      }
    });
  }

  static ParDo.SingleOutput<Event, PubsubMessage> eventToMessage() {
    return ParDo.of(new DoFn<Event, PubsubMessage>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        try {
          byte[] payload = CoderUtils.encodeToByteArray(Event.CODER, c.element());
          c.output(new PubsubMessage(payload, new HashMap<String, String>()));
        } catch (CoderException e) {
          LOG.error("Error while serializing Event {} to Pubsub message", c.element(), e);
        }
      }
    });
  }
}
