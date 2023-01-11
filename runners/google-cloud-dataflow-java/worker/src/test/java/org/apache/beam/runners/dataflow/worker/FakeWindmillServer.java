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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An in-memory Windmill server that offers provided work and data. */
class FakeWindmillServer extends WindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(FakeWindmillServer.class);

  static class ResponseQueue<T, U> {
    private final Queue<Function<T, U>> responses = new ConcurrentLinkedQueue<>();
    private Function<T, U> defaultResponse;
    Duration sleep = Duration.ZERO;

    // (Fluent) interface for response producers, accessible from tests.

    ResponseQueue<T, U> thenAnswer(Function<T, U> mapFun) {
      responses.add(mapFun);
      return this;
    }

    ResponseQueue<T, U> thenReturn(U response) {
      return thenAnswer((request) -> response);
    }

    ResponseQueue<T, U> answerByDefault(Function<T, U> mapFun) {
      defaultResponse = mapFun;
      return this;
    }

    ResponseQueue<T, U> returnByDefault(U response) {
      return answerByDefault((request) -> response);
    }

    ResponseQueue<T, U> delayEachResponseBy(Duration sleep) {
      this.sleep = sleep;
      return this;
    }

    // Interface for response consumers, accessible from the enclosing class.

    private U getOrDefault(T request) {
      Function<T, U> mapFun = responses.poll();
      U response = mapFun == null ? defaultResponse.apply(request) : mapFun.apply(request);
      Uninterruptibles.sleepUninterruptibly(sleep.getMillis(), TimeUnit.MILLISECONDS);
      return response;
    }

    private U get(T request) {
      Function<T, U> mapFun = responses.poll();
      U response = mapFun == null ? null : mapFun.apply(request);
      Uninterruptibles.sleepUninterruptibly(sleep.getMillis(), TimeUnit.MILLISECONDS);
      return response;
    }

    private boolean isEmpty() {
      return responses.isEmpty();
    }
  }

  private final ResponseQueue<Windmill.GetWorkRequest, Windmill.GetWorkResponse> workToOffer;
  private final ResponseQueue<GetDataRequest, GetDataResponse> dataToOffer;
  private final ResponseQueue<Windmill.CommitWorkRequest, CommitWorkResponse> commitsToOffer;
  // Keys are work tokens.
  private final Map<Long, WorkItemCommitRequest> commitsReceived;
  private final ArrayList<Windmill.ReportStatsRequest> statsReceived;
  private final LinkedBlockingQueue<Windmill.Exception> exceptions;
  private int commitsRequested = 0;
  private int numGetDataRequests = 0;
  private final AtomicInteger expectedExceptionCount;
  private final ErrorCollector errorCollector;
  private boolean isReady = true;
  private boolean dropStreamingCommits = false;
  private final ConcurrentHashMap<Long, Consumer<Windmill.CommitStatus>> droppedStreamingCommits;

  public FakeWindmillServer(ErrorCollector errorCollector) {
    workToOffer =
        new ResponseQueue<Windmill.GetWorkRequest, Windmill.GetWorkResponse>()
            .returnByDefault(Windmill.GetWorkResponse.getDefaultInstance());
    dataToOffer =
        new ResponseQueue<GetDataRequest, GetDataResponse>()
            .returnByDefault(GetDataResponse.getDefaultInstance())
            // Sleep for a little bit to ensure that *-windmill-read state-sampled counters show up.
            .delayEachResponseBy(Duration.millis(500));
    commitsToOffer =
        new ResponseQueue<Windmill.CommitWorkRequest, CommitWorkResponse>()
            .returnByDefault(CommitWorkResponse.getDefaultInstance());
    commitsReceived = new ConcurrentHashMap<>();
    exceptions = new LinkedBlockingQueue<>();
    expectedExceptionCount = new AtomicInteger();
    this.errorCollector = errorCollector;
    statsReceived = new ArrayList<>();
    droppedStreamingCommits = new ConcurrentHashMap<>();
  }

  public void setDropStreamingCommits(boolean dropStreamingCommits) {
    this.dropStreamingCommits = dropStreamingCommits;
  }

  public ResponseQueue<Windmill.GetWorkRequest, Windmill.GetWorkResponse> whenGetWorkCalled() {
    return workToOffer;
  }

  public ResponseQueue<GetDataRequest, GetDataResponse> whenGetDataCalled() {
    return dataToOffer;
  }

  public ResponseQueue<Windmill.CommitWorkRequest, Windmill.CommitWorkResponse>
      whenCommitWorkCalled() {
    return commitsToOffer;
  }

  @Override
  public Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request) {
    LOG.debug("getWorkRequest: {}", request.toString());
    Windmill.GetWorkResponse response = workToOffer.getOrDefault(request);
    LOG.debug("getWorkResponse: {}", response.toString());
    return response;
  }

  private void validateGetDataRequest(Windmill.GetDataRequest request) {
    for (ComputationGetDataRequest computationRequest : request.getRequestsList()) {
      for (KeyedGetDataRequest keyRequest : computationRequest.getRequestsList()) {
        errorCollector.checkThat(keyRequest.hasWorkToken(), equalTo(true));
        errorCollector.checkThat(
            keyRequest.getShardingKey(), allOf(greaterThan(0L), lessThan(Long.MAX_VALUE)));
        errorCollector.checkThat(keyRequest.getMaxBytes(), greaterThanOrEqualTo(0L));
      }
    }
  }

  @Override
  public Windmill.GetDataResponse getData(Windmill.GetDataRequest request) {
    LOG.info("getDataRequest: {}", request.toString());
    validateGetDataRequest(request);
    ++numGetDataRequests;
    GetDataResponse response = dataToOffer.getOrDefault(request);
    LOG.debug("getDataResponse: {}", response.toString());
    return response;
  }

  private void validateCommitWorkRequest(Windmill.CommitWorkRequest request) {
    for (ComputationCommitWorkRequest computationRequest : request.getRequestsList()) {
      for (WorkItemCommitRequest commit : computationRequest.getRequestsList()) {
        errorCollector.checkThat(commit.hasWorkToken(), equalTo(true));
        errorCollector.checkThat(
            commit.getShardingKey(), allOf(greaterThan(0L), lessThan(Long.MAX_VALUE)));
        errorCollector.checkThat(commit.getCacheToken(), not(equalTo(0L)));
      }
    }
  }

  @Override
  public CommitWorkResponse commitWork(Windmill.CommitWorkRequest request) {
    LOG.debug("commitWorkRequest: {}", request);
    validateCommitWorkRequest(request);
    for (ComputationCommitWorkRequest computationRequest : request.getRequestsList()) {
      for (WorkItemCommitRequest commit : computationRequest.getRequestsList()) {
        commitsReceived.put(commit.getWorkToken(), commit);
      }
    }
    CommitWorkResponse response = commitsToOffer.getOrDefault(request);
    LOG.debug("commitWorkResponse: {}", response);
    return response;
  }

  @Override
  public Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request) {
    return Windmill.GetConfigResponse.newBuilder().build();
  }

  @Override
  public Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request) {
    for (Windmill.Exception exception : request.getExceptionsList()) {
      Uninterruptibles.putUninterruptibly(exceptions, exception);
    }

    statsReceived.add(request);
    if (request.getExceptionsList().isEmpty() || expectedExceptionCount.getAndDecrement() > 0) {
      return Windmill.ReportStatsResponse.newBuilder().build();
    } else {
      return Windmill.ReportStatsResponse.newBuilder().setFailed(true).build();
    }
  }

  @Override
  public long getAndResetThrottleTime() {
    return (long) 0;
  }

  @Override
  public GetWorkStream getWorkStream(Windmill.GetWorkRequest request, WorkItemReceiver receiver) {
    LOG.debug("getWorkStream: {}", request.toString());
    Instant startTime = Instant.now();
    final CountDownLatch done = new CountDownLatch(1);
    return new GetWorkStream() {
      @Override
      public void close() {
        done.countDown();
      }

      @Override
      public boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException {
        while (done.getCount() > 0) {
          Windmill.GetWorkResponse response = workToOffer.get(null);
          if (response == null) {
            try {
              sleepMillis(500);
            } catch (InterruptedException e) {
              close();
              Thread.currentThread().interrupt();
            }
            continue;
          }
          for (Windmill.ComputationWorkItems computationWork : response.getWorkList()) {
            Instant inputDataWatermark =
                WindmillTimeUtils.windmillToHarnessWatermark(
                    computationWork.getInputDataWatermark());
            for (Windmill.WorkItem workItem : computationWork.getWorkList()) {
              receiver.receiveWork(
                  computationWork.getComputationId(), inputDataWatermark, Instant.now(), workItem);
            }
          }
        }
        return done.await(time, unit);
      }

      @Override
      public Instant startTime() {
        return startTime;
      }
    };
  }

  @Override
  public GetDataStream getDataStream() {
    Instant startTime = Instant.now();
    return new GetDataStream() {
      @Override
      public Windmill.KeyedGetDataResponse requestKeyedData(
          String computation, KeyedGetDataRequest request) {
        Windmill.GetDataRequest getDataRequest =
            GetDataRequest.newBuilder()
                .addRequests(
                    ComputationGetDataRequest.newBuilder()
                        .setComputationId(computation)
                        .addRequests(request)
                        .build())
                .build();
        GetDataResponse getDataResponse = getData(getDataRequest);
        if (getDataResponse.getDataList().isEmpty()) {
          return null;
        }
        assertEquals(1, getDataResponse.getDataCount());
        if (getDataResponse.getData(0).getDataList().isEmpty()) {
          return null;
        }
        assertEquals(1, getDataResponse.getData(0).getDataCount());
        return getDataResponse.getData(0).getData(0);
      }

      @Override
      public Windmill.GlobalData requestGlobalData(Windmill.GlobalDataRequest request) {
        Windmill.GetDataRequest getDataRequest =
            GetDataRequest.newBuilder().addGlobalDataFetchRequests(request).build();
        GetDataResponse getDataResponse = getData(getDataRequest);
        if (getDataResponse.getGlobalDataList().isEmpty()) {
          return null;
        }
        assertEquals(1, getDataResponse.getGlobalDataCount());
        return getDataResponse.getGlobalData(0);
      }

      @Override
      public void refreshActiveWork(Map<String, List<KeyedGetDataRequest>> active) {
        Windmill.GetDataRequest.Builder builder = Windmill.GetDataRequest.newBuilder();
        for (Map.Entry<String, List<KeyedGetDataRequest>> entry : active.entrySet()) {
          builder.addRequests(
              ComputationGetDataRequest.newBuilder()
                  .setComputationId(entry.getKey())
                  .addAllRequests(entry.getValue()));
        }
        getData(builder.build());
      }

      @Override
      public void close() {}

      @Override
      public boolean awaitTermination(int time, TimeUnit unit) {
        return true;
      }

      @Override
      public Instant startTime() {
        return startTime;
      }
    };
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    Instant startTime = Instant.now();
    return new CommitWorkStream() {
      @Override
      public boolean commitWorkItem(
          String computation,
          WorkItemCommitRequest request,
          Consumer<Windmill.CommitStatus> onDone) {
        LOG.debug("commitWorkStream::commitWorkItem: {}", request);
        errorCollector.checkThat(request.hasWorkToken(), equalTo(true));
        errorCollector.checkThat(
            request.getShardingKey(), allOf(greaterThan(0L), lessThan(Long.MAX_VALUE)));
        errorCollector.checkThat(request.getCacheToken(), not(equalTo(0L)));
        // Throws away the result, but allows to inject latency.
        Windmill.CommitWorkRequest.Builder builder = Windmill.CommitWorkRequest.newBuilder();
        builder.addRequestsBuilder().setComputationId(computation).addRequests(request);
        commitsToOffer.getOrDefault(builder.build());
        if (dropStreamingCommits) {
          droppedStreamingCommits.put(request.getWorkToken(), onDone);
        } else {
          commitsReceived.put(request.getWorkToken(), request);
          onDone.accept(Windmill.CommitStatus.OK);
        }
        // Return true to indicate the request was accepted even if we are dropping the commit
        // to simulate a dropped commit.
        return true;
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}

      @Override
      public boolean awaitTermination(int time, TimeUnit unit) {
        return true;
      }

      @Override
      public Instant startTime() {
        return startTime;
      }
    };
  }

  public void waitForEmptyWorkQueue() {
    while (!workToOffer.isEmpty()) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
  }

  public Map<Long, WorkItemCommitRequest> waitForAndGetCommits(int numCommits) {
    LOG.debug("waitForAndGetCommitsRequest: {}", numCommits);
    int maxTries = 10;
    while (maxTries-- > 0 && commitsReceived.size() < commitsRequested + numCommits) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }

    assertFalse(
        "Should have received "
            + numCommits
            + " more commits beyond "
            + commitsRequested
            + " commits already seen, but after 10s have only seen "
            + commitsReceived
            + ". Exceptions seen: "
            + exceptions,
        commitsReceived.size() < commitsRequested + numCommits);
    commitsRequested += numCommits;

    LOG.debug("waitForAndGetCommitsResponse: {}", commitsReceived);
    return commitsReceived;
  }

  public void clearCommitsReceived() {
    commitsRequested = 0;
    commitsReceived.clear();
  }

  public ConcurrentHashMap<Long, Consumer<Windmill.CommitStatus>> waitForDroppedCommits(
      int droppedCommits) {
    LOG.debug("waitForDroppedCommits: {}", droppedCommits);
    int maxTries = 10;
    while (maxTries-- > 0 && droppedStreamingCommits.size() < droppedCommits) {
      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
    assertEquals(droppedCommits, droppedStreamingCommits.size());
    return droppedStreamingCommits;
  }

  public void setExpectedExceptionCount(int i) {
    expectedExceptionCount.getAndAdd(i);
  }

  public Windmill.Exception getException() throws InterruptedException {
    return exceptions.take();
  }

  public int numGetDataRequests() {
    return numGetDataRequests;
  }

  public ArrayList<Windmill.ReportStatsRequest> getStatsReceived() {
    return statsReceived;
  }

  @Override
  public void setWindmillServiceEndpoints(Set<HostAndPort> endpoints) throws IOException {
    isReady = true;
  }

  @Override
  public boolean isReady() {
    return isReady;
  }

  public void setIsReady(boolean ready) {
    this.isReady = ready;
  }
}
