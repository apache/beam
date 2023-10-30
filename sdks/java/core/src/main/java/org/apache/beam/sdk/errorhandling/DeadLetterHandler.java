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
package org.apache.beam.sdk.errorhandling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DeadLetterHandler {

  DeadLetterHandler THROWING_HANDLER = new ThrowingDeadLetterHandler();

  DeadLetterHandler RECORDING_HANDLER = new RecordingDeadLetterHandler();

  TupleTag<DeadLetter> DEAD_LETTER_TAG = new TupleTag<>();

  <T> void handle(
      DoFn<?, ?>.ProcessContext c,
      T record,
      @Nullable Coder<T> coder,
      @Nullable Exception exception,
      String description,
      String failingTransform)
      throws Exception;

  class ThrowingDeadLetterHandler implements DeadLetterHandler {

    @Override
    public <T> void handle(
        DoFn<?, ?>.ProcessContext c,
        T record,
        @Nullable Coder<T> coder,
        @Nullable Exception exception,
        String description,
        String failingTransform)
        throws Exception {
      if (exception != null) {
        throw exception;
      }
    }
  }

  class RecordingDeadLetterHandler implements DeadLetterHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RecordingDeadLetterHandler.class);

    @Override
    public <T> void handle(
        DoFn<?, ?>.ProcessContext c,
        T record,
        @Nullable Coder<T> coder,
        @Nullable Exception exception,
        String description,
        String failingTransform)
        throws Exception {
      Preconditions.checkArgumentNotNull(record);
      ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();

      DeadLetter.Builder deadLetterBuilder =
          DeadLetter.builder()
              .setHumanReadableRecord(objectWriter.writeValueAsString(record))
              .setDescription(description)
              .setFailingTransform(failingTransform);

      // Its possible for us to want to handle an error scenario where no actual exception objet
      // exists
      if (exception != null) {
        deadLetterBuilder.setException(exception.toString());
      }

      // We will sometimes not have a coder for a failing record, for example if it has already been
      // modified within the dofn.
      if (coder != null) {
        deadLetterBuilder.setCoder(coder.toString());

        try {
          ByteArrayOutputStream stream = new ByteArrayOutputStream();
          coder.encode(record, stream);
          byte[] bytes = stream.toByteArray();
          deadLetterBuilder.setEncodedRecord(bytes);
        } catch (IOException e) {
          LOG.error(
              "Unable to encode failing record using provided coder."
                  + " DeadLetter will be published without encoded bytes",
              e);
        }
      }
      DeadLetter deadLetter = deadLetterBuilder.build();
      c.output(DEAD_LETTER_TAG, deadLetter);
    }
  }
}
