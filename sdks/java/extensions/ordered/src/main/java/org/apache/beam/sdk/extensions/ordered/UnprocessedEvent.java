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
package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Combines the source event which failed to process with the failure reason.
 *
 * @param <EventT>
 */
@AutoValue
public abstract class UnprocessedEvent<EventT> {

  /**
   * Create new unprocessed event.
   *
   * @param event failed event
   * @param reason for failure
   * @param <EventT> type of the event
   * @return
   */
  public static <EventT> UnprocessedEvent<EventT> create(EventT event, Reason reason) {
    return new AutoValue_UnprocessedEvent<>(event, reason, null);
  }

  /**
   * Create new unprocessed event which failed due to an exception thrown.
   *
   * @param event which failed
   * @param exception which caused the failure
   * @param <EventT> type of the event
   * @return
   */
  public static <EventT> UnprocessedEvent<EventT> create(EventT event, Exception exception) {
    return new AutoValue_UnprocessedEvent<>(
        event, Reason.exception_thrown, ExceptionUtils.getStackTrace(exception));
  }

  static <EventT> UnprocessedEvent<EventT> create(
      EventT event, Reason reason, @Nullable String failureDetails) {
    return new AutoValue_UnprocessedEvent<>(event, reason, failureDetails);
  }

  public enum Reason {
    duplicate,
    buffered,
    sequence_id_outside_valid_range,
    exception_thrown,
    before_initial_sequence
  };

  public abstract EventT getEvent();

  public abstract Reason getReason();

  public abstract @Nullable String getExplanation();

  static class UnprocessedEventCoder<EventT> extends Coder<UnprocessedEvent<EventT>> {

    private final Coder<EventT> eventCoder;
    private final NullableCoder<String> explanationCoder = NullableCoder.of(StringUtf8Coder.of());

    UnprocessedEventCoder(Coder<EventT> eventCoder) {
      this.eventCoder = eventCoder;
    }

    @Override
    public void encode(UnprocessedEvent<EventT> value, OutputStream outStream) throws IOException {
      ByteCoder.of().encode((byte) value.getReason().ordinal(), outStream);
      explanationCoder.encode(value.getExplanation(), outStream);
      eventCoder.encode(value.getEvent(), outStream);
    }

    @Override
    public UnprocessedEvent<EventT> decode(InputStream inputStream) throws IOException {
      Reason reason = Reason.values()[ByteCoder.of().decode(inputStream)];
      String explanation = explanationCoder.decode(inputStream);
      EventT event = eventCoder.decode(inputStream);
      return UnprocessedEvent.create(event, reason, explanation);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(eventCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "Unprocessed event coder requires deterministic event coder", eventCoder);
    }
  }
}
