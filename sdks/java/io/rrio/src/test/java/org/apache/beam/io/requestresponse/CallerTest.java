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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Caller}. */
@RunWith(JUnit4.class)
public class CallerTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void canSerializeImplementingClasses() {
    SerializableUtils.serializeToByteArray(new CallerImpl());
  }

  @Test
  public void canSerializeWhenUsedInDoFn() {
    pipeline
        .apply(Create.of(Instant.now()))
        .apply(ParDo.of(new CallerUsingDoFn<>(new CallerImpl())))
        .setCoder(StringUtf8Coder.of());

    pipeline.run();
  }

  @Test
  public void canSignalQuotaException() {
    pipeline
        .apply(Create.of(1))
        .apply(ParDo.of(new CallerUsingDoFn<>(new CallerThrowsQuotaException())))
        .setCoder(VarIntCoder.of());

    PipelineExecutionException executionException =
        assertThrows(PipelineExecutionException.class, pipeline::run);
    assertEquals(UserCodeQuotaException.class, executionException.getCause().getClass());
  }

  @Test
  public void canSignalTimeoutException() {
    pipeline
        .apply(Create.of(1))
        .apply(ParDo.of(new CallerUsingDoFn<>(new CallerThrowsTimeoutException())))
        .setCoder(VarIntCoder.of());

    PipelineExecutionException executionException =
        assertThrows(PipelineExecutionException.class, pipeline::run);
    assertEquals(UserCodeTimeoutException.class, executionException.getCause().getClass());
  }

  private static class CallerUsingDoFn<RequestT, ResponseT> extends DoFn<RequestT, ResponseT> {
    private final Caller<RequestT, ResponseT> caller;

    private CallerUsingDoFn(Caller<RequestT, ResponseT> caller) {
      this.caller = caller;
    }

    @ProcessElement
    public void process(@Element RequestT request, OutputReceiver<ResponseT> receiver)
        throws UserCodeExecutionException {
      RequestT safeRequest = checkStateNotNull(request);
      ResponseT response = caller.call(safeRequest);
      receiver.output(response);
    }
  }

  private static class CallerImpl implements Caller<Instant, String> {

    @Override
    public String call(Instant request) throws UserCodeExecutionException {
      return request.toString();
    }
  }

  private static class CallerThrowsQuotaException implements Caller<Integer, Integer> {

    @Override
    public Integer call(Integer request) throws UserCodeExecutionException {
      throw new UserCodeQuotaException("quota");
    }
  }

  private static class CallerThrowsTimeoutException implements Caller<Integer, Integer> {

    @Override
    public Integer call(Integer request) throws UserCodeExecutionException {
      throw new UserCodeTimeoutException("timeout");
    }
  }
}
