/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
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
import static org.junit.Assert.assertFalse;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.rules.ErrorCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An in-memory Windmill server that offers provided work and data. */
class FakeWindmillServer extends WindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(FakeWindmillServer.class);

  private final Queue<Windmill.GetWorkResponse> workToOffer;
  private final Queue<Function<GetDataRequest, GetDataResponse>> dataToOffer;
  private final Map<Long, WorkItemCommitRequest> commitsReceived;
  private final ArrayList<Windmill.ReportStatsRequest> statsReceived;
  private final LinkedBlockingQueue<Windmill.Exception> exceptions;
  private int commitsRequested = 0;
  private int numGetDataRequests = 0;
  private final AtomicInteger expectedExceptionCount;
  private final ErrorCollector errorCollector;
  private boolean isReady = true;

  public FakeWindmillServer(ErrorCollector errorCollector) {
    workToOffer = new ConcurrentLinkedQueue<>();
    dataToOffer = new ConcurrentLinkedQueue<>();
    commitsReceived = new ConcurrentHashMap<>();
    exceptions = new LinkedBlockingQueue<>();
    expectedExceptionCount = new AtomicInteger();
    this.errorCollector = errorCollector;
    statsReceived = new ArrayList<>();
  }

  public void addWorkToOffer(Windmill.GetWorkResponse work) {
    workToOffer.add(work);
  }

  public void addDataToOffer(Windmill.GetDataResponse data) {
    dataToOffer.add((GetDataRequest request) -> data);
  }

  public void addDataFnToOffer(Function<GetDataRequest, GetDataResponse> f) {
    dataToOffer.add(f);
  }

  @Override
  public Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request) {
    LOG.debug("getWorkRequest: {}", request.toString());
    Windmill.GetWorkResponse response = workToOffer.poll();
    if (response == null) {
      return Windmill.GetWorkResponse.newBuilder().build();
    }
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
    GetDataResponse response;
    Function<GetDataRequest, GetDataResponse> responseFn = dataToOffer.poll();
    if (responseFn == null) {
      response = Windmill.GetDataResponse.newBuilder().build();
    } else {
      response = responseFn.apply(request);
      try {
        // Sleep for a little bit to ensure that *-windmill-read state-sampled counters
        // show up.
        sleepMillis(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
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
    CommitWorkResponse response = CommitWorkResponse.newBuilder().build();
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
  public GetWorkStream getWorkStream(Windmill.GetWorkRequest request, WorkItemReceiver receiver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetDataStream getDataStream() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    throw new UnsupportedOperationException();
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
