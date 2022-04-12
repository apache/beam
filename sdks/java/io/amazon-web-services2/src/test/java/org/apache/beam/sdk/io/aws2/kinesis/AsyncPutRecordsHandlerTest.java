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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.util.Collections.emptyList;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.beam.sdk.util.BackOff;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class AsyncPutRecordsHandlerTest extends PutRecordsHelpers {
  private static final String STREAM = "streamName";
  private static final int CONCURRENCY = 10;

  private CompletableFuture<PutRecordsResponse> pendingResponse = new CompletableFuture<>();

  @Mock private KinesisAsyncClient client;
  @Mock private Supplier<BackOff> backoff;
  private AsyncPutRecordsHandler handler;

  @Before
  public void init() {
    handler =
        new AsyncPutRecordsHandler(
            client, CONCURRENCY, backoff, mock(AsyncPutRecordsHandler.Stats.class));
    when(client.putRecords(anyRequest())).thenReturn(pendingResponse);
  }

  @Test
  public void retryOnPartialSuccess() throws Throwable {
    when(backoff.get()).thenReturn(BackOff.ZERO_BACKOFF);
    CompletableFuture<PutRecordsResponse> pendingResponse2 = new CompletableFuture<>();
    CompletableFuture<PutRecordsResponse> pendingResponse3 = new CompletableFuture<>();
    when(client.putRecords(anyRequest()))
        .thenReturn(pendingResponse, pendingResponse2, pendingResponse3);

    List<PutRecordsRequestEntry> records = fromTestRows(getExpectedValues(0, 100));
    handler.putRecords(STREAM, records);

    // 1st attempt
    eventually(5, () -> verify(client, times(1)).putRecords(anyRequest()));
    verify(client).putRecords(request(records));
    assertThat(handler.pendingRequests()).isEqualTo(1);

    // 2nd attempt
    pendingResponse.complete(partialSuccessResponse(50, 50));
    eventually(5, () -> verify(client, times(2)).putRecords(anyRequest()));
    verify(client).putRecords(request(records.subList(50, 100)));
    assertThat(handler.pendingRequests()).isEqualTo(1);

    // 3rd attempt
    pendingResponse2.complete(partialSuccessResponse(25, 25));
    eventually(5, () -> verify(client, times(3)).putRecords(anyRequest()));
    verify(client).putRecords(request(records.subList(75, 100)));
    assertThat(handler.pendingRequests()).isEqualTo(1);

    // 4th attempt
    pendingResponse3.complete(PutRecordsResponse.builder().build()); // success
    verifyNoMoreInteractions(client);

    eventually(5, () -> assertThat(handler.pendingRequests()).isEqualTo(0));
  }

  @Test
  public void retryLimitOnPartialSuccess() throws Throwable {
    when(backoff.get()).thenReturn(BackOff.STOP_BACKOFF);

    List<PutRecordsRequestEntry> records = fromTestRows(getExpectedValues(0, 100));
    handler.putRecords(STREAM, records);

    pendingResponse.complete(partialSuccessResponse(98, 2));

    assertThatThrownBy(() -> handler.waitForCompletion())
        .hasMessageContaining("Exceeded retries")
        .hasMessageEndingWith(ERROR_CODE + " for 2 record(s).")
        .isInstanceOf(IOException.class);
    verify(client).putRecords(anyRequest());
  }

  @Test
  public void propagateErrorOnPutRecords() throws Throwable {
    handler.putRecords(STREAM, emptyList());
    pendingResponse.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.putRecords(STREAM, emptyList())).hasMessage("Request failed");
    assertThat(handler.hasErrored()).isTrue();
    verify(client).putRecords(anyRequest());
  }

  @Test
  public void propagateErrorWhenPolling() throws Throwable {
    handler.putRecords(STREAM, emptyList());
    handler.checkForAsyncFailure(); // none yet
    pendingResponse.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.checkForAsyncFailure()).hasMessage("Request failed");
    assertThat(handler.hasErrored()).isTrue();
    handler.checkForAsyncFailure(); // already reset
  }

  @Test
  public void propagateErrorOnWaitForCompletion() throws Throwable {
    handler.putRecords(STREAM, emptyList());
    pendingResponse.completeExceptionally(new RuntimeException("Request failed"));

    assertThatThrownBy(() -> handler.waitForCompletion()).hasMessage("Request failed");
  }

  @Test
  public void correctlyLimitConcurrency() throws Throwable {
    // exhaust concurrency limit so that putRecords blocks
    Runnable task = repeat(CONCURRENCY + 1, () -> handler.putRecords(STREAM, emptyList()));
    Future<?> future = commonPool().submit(task);

    eventually(5, () -> assertThat(handler.pendingRequests()).isEqualTo(CONCURRENCY));
    eventually(5, () -> verify(client, times(CONCURRENCY)).putRecords(anyRequest()));
    assertThat(future).isNotDone();

    // complete responses and unblock last request
    pendingResponse.complete(PutRecordsResponse.builder().build());

    eventually(5, () -> verify(client, times(CONCURRENCY + 1)).putRecords(anyRequest()));
    handler.waitForCompletion();
    assertThat(future).isDone();
  }

  private PutRecordsRequest request(List<PutRecordsRequestEntry> records) {
    return PutRecordsRequest.builder().streamName(STREAM).records(records).build();
  }

  private void eventually(int attempts, Runnable fun) {
    for (int i = 0; i < attempts - 1; i++) {
      try {
        Thread.sleep(i * 100);
        fun.run();
        return;
      } catch (AssertionError | InterruptedException t) {
      }
    }
    fun.run();
  }

  private Runnable repeat(int times, ThrowingRunnable fun) {
    return () -> {
      for (int i = 0; i < times; i++) {
        try {
          fun.run();
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    };
  }
}
