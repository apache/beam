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

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord.Record;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BadRecordRouterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private MultiOutputReceiver outputReceiver;

  @Mock private OutputReceiver<BadRecord> badRecordOutputReceiver;

  private static final BiFunction<
          BadRecord.Builder, BadRecord.Failure.Builder, ArgumentMatcher<BadRecord>>
      ignoreStacktraceMatcher =
          (expectedBuilder, failure) ->
              (ArgumentMatcher<BadRecord>)
                  argument -> {
                    // This complex matcher means we don't need to maintain an expected stacktrace
                    String stackTrace = argument.getFailure().getExceptionStacktrace();
                    failure.setExceptionStacktrace(stackTrace);
                    BadRecord expected = expectedBuilder.setFailure(failure.build()).build();
                    return expected.equals(argument);
                  };

  @Test
  public void testThrowingHandlerWithException() throws Exception {
    BadRecordRouter handler = BadRecordRouter.THROWING_ROUTER;

    thrown.expect(IOException.class);

    handler.route(outputReceiver, new Object(), null, new IOException(), "desc");
  }

  @Test
  public void testThrowingHandlerWithNoException() throws Exception {
    BadRecordRouter handler = BadRecordRouter.THROWING_ROUTER;

    thrown.expect(RuntimeException.class);

    handler.route(outputReceiver, new Object(), null, null, "desc");
  }

  @Test
  public void testRecordingHandler() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    handler.route(outputReceiver, 5, BigEndianIntegerCoder.of(), new RuntimeException(), "desc");

    BadRecord.Builder expectedBuilder =
        BadRecord.builder()
            .setRecord(
                Record.builder()
                    .setHumanReadableJsonRecord("5")
                    .setEncodedRecord(new byte[] {0, 0, 0, 5})
                    .setCoder("BigEndianIntegerCoder")
                    .build());

    BadRecord.Failure.Builder failure =
        BadRecord.Failure.builder()
            .setException("java.lang.RuntimeException")
            .setDescription("desc");

    verify(badRecordOutputReceiver)
        .output(ArgumentMatchers.argThat(ignoreStacktraceMatcher.apply(expectedBuilder, failure)));
  }

  @Test
  public void testNoCoder() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    handler.route(outputReceiver, 5, null, new RuntimeException(), "desc");

    BadRecord.Builder expectedBuilder =
        BadRecord.builder().setRecord(Record.builder().setHumanReadableJsonRecord("5").build());

    BadRecord.Failure.Builder failure =
        BadRecord.Failure.builder()
            .setException("java.lang.RuntimeException")
            .setDescription("desc");

    verify(badRecordOutputReceiver)
        .output(ArgumentMatchers.argThat(ignoreStacktraceMatcher.apply(expectedBuilder, failure)));
  }

  @Test
  public void testFailingCoder() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    Coder<Integer> failingCoder =
        new Coder<Integer>() {
          @Override
          public void encode(Integer value, OutputStream outStream)
              throws CoderException, IOException {
            throw new CoderException("Failing Coder");
          }

          @Override
          public Integer decode(InputStream inStream) throws CoderException, IOException {
            return null;
          }

          @Override
          public List<? extends Coder<?>> getCoderArguments() {
            return null;
          }

          @Override
          public void verifyDeterministic() throws NonDeterministicException {}
        };

    handler.route(outputReceiver, 5, failingCoder, new RuntimeException(), "desc");

    BadRecord.Builder expectedBuilder =
        BadRecord.builder()
            .setRecord(
                Record.builder()
                    .setHumanReadableJsonRecord("5")
                    .setCoder(failingCoder.toString())
                    .build());

    BadRecord.Failure.Builder failure =
        BadRecord.Failure.builder()
            .setException("java.lang.RuntimeException")
            .setDescription("desc");

    verify(badRecordOutputReceiver)
        .output(ArgumentMatchers.argThat(ignoreStacktraceMatcher.apply(expectedBuilder, failure)));
  }
}
