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

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface BadRecordRouter extends Serializable {

  BadRecordRouter THROWING_ROUTER = new ThrowingBadRecordRouter();

  BadRecordRouter RECORDING_ROUTER = new RecordingBadRecordRouter();

  TupleTag<BadRecord> BAD_RECORD_TAG = new TupleTag<>();

  <RecordT> void route(
      MultiOutputReceiver outputReceiver,
      RecordT record,
      @Nullable Coder<RecordT> coder,
      @Nullable Exception exception,
      String description)
      throws Exception;

  <RecordT> void route(
      DoFn<?, ?>.FinishBundleContext c,
      RecordT record,
      @Nullable Coder<RecordT> coder,
      @Nullable Exception exception,
      String description,
      BoundedWindow window)
      throws Exception;

  class ThrowingBadRecordRouter implements BadRecordRouter {

    @Override
    public <RecordT> void route(
        MultiOutputReceiver outputReceiver,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description)
        throws Exception {
      route(record, exception);
    }

    @Override
    public <RecordT> void route(
        DoFn<?, ?>.FinishBundleContext c,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description,
        BoundedWindow window)
        throws Exception {
      route(record, exception);
    }

    private <RecordT> void route(RecordT record, @Nullable Exception exception) throws Exception {
      if (exception != null) {
        throw exception;
      } else {
        Preconditions.checkArgumentNotNull(record);
        String encodedRecord =
            BadRecord.Record.builder()
                .addHumanReadableJson(record)
                .build()
                .getHumanReadableJsonRecord();
        if (encodedRecord == null) {
          encodedRecord = "Unable to serialize bad record";
        }
        throw new RuntimeException("Encountered Bad Record: " + encodedRecord);
      }
    }
  }

  class RecordingBadRecordRouter implements BadRecordRouter {

    @Override
    public <RecordT> void route(
        MultiOutputReceiver outputReceiver,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description)
        throws Exception {
      outputReceiver
          .get(BAD_RECORD_TAG)
          .output(BadRecord.fromExceptionInformation(record, coder, exception, description));
    }

    @Override
    public <RecordT> void route(
        DoFn<?, ?>.FinishBundleContext c,
        RecordT record,
        @Nullable Coder<RecordT> coder,
        @Nullable Exception exception,
        String description,
        BoundedWindow window)
        throws Exception {
      c.output(
          BAD_RECORD_TAG,
          BadRecord.fromExceptionInformation(record, coder, exception, description),
          window.maxTimestamp(),
          window);
    }
  }
}
