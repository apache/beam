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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.hamcrest.Matchers;
import org.junit.rules.ErrorCollector;

class FakeWindmillGrpcService
    extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
  private final ErrorCollector errorCollector;

  @GuardedBy("this")
  private boolean noMoreStreamsExpected = false;

  @GuardedBy("this")
  private int failedStreamConnectsRemaining = 0;

  public FakeWindmillGrpcService(ErrorCollector errorCollector) {
    this.errorCollector = errorCollector;
  }

  @SuppressWarnings("BusyWait")
  public void waitForFailedConnectAttempts() throws InterruptedException {
    while (true) {
      Thread.sleep(2);
      synchronized (this) {
        if (failedStreamConnectsRemaining <= 0) {
          break;
        }
      }
    }
  }

  public synchronized void setFailedStreamConnectsRemaining(int failedStreamConnectsRemaining) {
    this.failedStreamConnectsRemaining = failedStreamConnectsRemaining;
  }

  public static class StreamInfo<RequestT, ResponseT> {
    public StreamInfo(StreamObserver<ResponseT> responseObserver) {
      this.responseObserver = responseObserver;
    }

    public final StreamObserver<ResponseT> responseObserver;
    public final BlockingQueue<RequestT> requests = new LinkedBlockingQueue<>(1000);
    public final CompletableFuture<Throwable> onDone = new CompletableFuture<>();
  };

  private final BlockingQueue<CommitStreamInfo> commitStreams = new LinkedBlockingQueue<>(1000);
  private final BlockingQueue<GetDataStreamInfo> getDataStreams = new LinkedBlockingQueue<>(1000);

  private static class StreamInfoObserver<RequestT, ResponseT> implements StreamObserver<RequestT> {
    private final StreamInfo<RequestT, ResponseT> streamInfo;
    private final ErrorCollector errorCollector;

    public StreamInfoObserver(
        StreamInfo<RequestT, ResponseT> streamInfo, ErrorCollector errorCollector) {
      this.streamInfo = streamInfo;
      this.errorCollector = errorCollector;
    }

    @Override
    public void onNext(RequestT request) {
      if (streamInfo.onDone.isDone()) {
        try {
          if (streamInfo.onDone.get() == null) {
            throw new IllegalStateException("Stream already half-closed.");
          } else {
            throw new IllegalStateException("Stream already closed with error.");
          }
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      errorCollector.checkThat(streamInfo.requests.add(request), Matchers.is(true));
    }

    @Override
    public void onError(Throwable throwable) {
      streamInfo.onDone.complete(throwable);
    }

    @Override
    public void onCompleted() {
      streamInfo.onDone.complete(null);
    }
  }

  public static class CommitStreamInfo
      extends StreamInfo<Windmill.StreamingCommitWorkRequest, Windmill.StreamingCommitResponse> {
    CommitStreamInfo(StreamObserver<Windmill.StreamingCommitResponse> responseObserver) {
      super(responseObserver);
    }
  }

  @Override
  public StreamObserver<Windmill.StreamingCommitWorkRequest> commitWorkStream(
      StreamObserver<Windmill.StreamingCommitResponse> responseObserver) {
    CommitStreamInfo info = new CommitStreamInfo(responseObserver);
    synchronized (this) {
      errorCollector.checkThat(noMoreStreamsExpected, Matchers.is(false));
      if (failedStreamConnectsRemaining-- > 0) {
        throw new RuntimeException(
            "Injected connection error, remaining failures: " + failedStreamConnectsRemaining);
      }
      errorCollector.checkThat(commitStreams.offer(info), Matchers.is(true));
    }
    return new StreamInfoObserver<>(info, errorCollector);
  }

  public CommitStreamInfo waitForConnectedCommitStream() throws InterruptedException {
    return commitStreams.take();
  }

  public synchronized void expectNoMoreStreams() {
    noMoreStreamsExpected = true;
    errorCollector.checkThat(commitStreams.isEmpty(), Matchers.is(true));
    errorCollector.checkThat(getDataStreams.isEmpty(), Matchers.is(true));
  }

  public static class GetDataStreamInfo
      extends StreamInfo<Windmill.StreamingGetDataRequest, Windmill.StreamingGetDataResponse> {
    GetDataStreamInfo(StreamObserver<Windmill.StreamingGetDataResponse> responseObserver) {
      super(responseObserver);
    }
  }

  @Override
  public StreamObserver<Windmill.StreamingGetDataRequest> getDataStream(
      StreamObserver<Windmill.StreamingGetDataResponse> responseObserver) {
    GetDataStreamInfo info = new GetDataStreamInfo(responseObserver);
    synchronized (this) {
      errorCollector.checkThat(noMoreStreamsExpected, Matchers.is(false));
      if (failedStreamConnectsRemaining-- > 0) {
        throw new RuntimeException(
            "Injected connection error, remaining failures: " + failedStreamConnectsRemaining);
      }
      errorCollector.checkThat(getDataStreams.offer(info), Matchers.is(true));
    }
    return new StreamInfoObserver<>(info, errorCollector);
  }

  public GetDataStreamInfo waitForConnectedGetDataStream() throws InterruptedException {
    return getDataStreams.take();
  }
}
