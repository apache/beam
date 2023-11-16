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
package org.apache.beam.sdk.transforms.errorhandling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord.Failure;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord.Record;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface BadRecordRouter extends Serializable {

  BadRecordRouter THROWING_ROUTER = new ThrowingBadRecordRouter();

  BadRecordRouter RECORDING_ROUTER = new RecordingBadRecordRouter();

  TupleTag<BadRecord> BAD_RECORD_TAG = new TupleTag<>();

  <RecordT> void route(
      MultiOutputReceiver outputReceiver,
      RecordT record,
      @Nullable Coder<RecordT> coder,
      @Nullable Exception exception,
      String description,
      String failingTransform)
      throws Exception;

  class ThrowingBadRecordRouter implements BadRecordRouter {

    @Override
    public <RecordT> void route(
        MultiOutputReceiver outputReceiver,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description,
        String failingTransform)
        throws Exception {
      if (exception != null) {
        throw exception;
      }
    }
  }

  class RecordingBadRecordRouter implements BadRecordRouter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordingBadRecordRouter.class);

    @Override
    public <RecordT> void route(
        MultiOutputReceiver outputReceiver,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description,
        String failingTransform)
        throws Exception {
      Preconditions.checkArgumentNotNull(record);
      ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();

      // Build up record information
      BadRecord.Record.Builder recordBuilder = Record.builder();
      try {
        recordBuilder.setJsonRecord(objectWriter.writeValueAsString(record));
      } catch (Exception e) {
        LOG.error(
            "Unable to serialize record as JSON. Human readable record attempted via .toString", e);
        try {
          recordBuilder.setJsonRecord(record.toString());
        } catch (Exception e2) {
          LOG.error(
              "Unable to serialize record via .toString. Human readable record will be null", e2);
        }
      }

      // We will sometimes not have a coder for a failing record, for example if it has already been
      // modified within the dofn.
      if (coder != null) {
        recordBuilder.setCoder(coder.toString());
        try {
          recordBuilder.setEncodedRecord(CoderUtils.encodeToByteArray(coder, record));
        } catch (IOException e) {
          LOG.error(
              "Unable to encode failing record using provided coder."
                  + " BadRecord will be published without encoded bytes",
              e);
        }
      }

      // Build up failure information
      BadRecord.Failure.Builder failureBuilder =
          Failure.builder().setDescription(description).setFailingTransform(failingTransform);

      // It's possible for us to want to handle an error scenario where no actual exception object
      // exists
      if (exception != null) {
        failureBuilder.setException(exception.toString());
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(stream, false, Charsets.UTF_8.name());
        exception.printStackTrace(printStream);
        printStream.close();
        failureBuilder.setExceptionStacktrace(new String(stream.toByteArray(), Charsets.UTF_8));
      }

      BadRecord badRecord =
          BadRecord.builder()
              .setRecord(recordBuilder.build())
              .setFailure(failureBuilder.build())
              .build();
      outputReceiver.get(BAD_RECORD_TAG).output(badRecord);
    }
  }
}
