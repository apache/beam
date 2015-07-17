/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertFalse;

import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.CommitWorkResponse;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.ComputationCommitWorkRequest;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.GetDataResponse;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.GetWorkResponse;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItemCommitRequest;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-memory Windmill server that offers provided work and data.
 */
class FakeWindmillServer extends WindmillServerStub {
  private Queue<Windmill.GetWorkResponse> workToOffer;
  private Queue<Windmill.GetDataResponse> dataToOffer;
  private Map<Long, WorkItemCommitRequest> commitsReceived;
  private LinkedBlockingQueue<Windmill.Exception> exceptions;
  private int commitsRequested = 0;
  private AtomicInteger expectedExceptionCount;
  public FakeWindmillServer() {
    workToOffer = new ConcurrentLinkedQueue<GetWorkResponse>();
    dataToOffer = new ConcurrentLinkedQueue<GetDataResponse>();
    commitsReceived = new ConcurrentHashMap<Long, WorkItemCommitRequest>();
    exceptions = new LinkedBlockingQueue<>();
    expectedExceptionCount = new AtomicInteger();
  }

  public void addWorkToOffer(Windmill.GetWorkResponse work) {
    workToOffer.add(work);
  }

  public void addDataToOffer(Windmill.GetDataResponse data) {
    dataToOffer.add(data);
  }

  @Override
  public Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request) {
    Windmill.GetWorkResponse response = workToOffer.poll();
    if (response == null) {
      return Windmill.GetWorkResponse.newBuilder().build();
    }
    return response;
  }

  @Override
  public Windmill.GetDataResponse getData(Windmill.GetDataRequest request) {
    Windmill.GetDataResponse response = dataToOffer.poll();
    if (response == null) {
      response = Windmill.GetDataResponse.newBuilder().build();
    }
    return response;
  }

  @Override
  public CommitWorkResponse commitWork(Windmill.CommitWorkRequest request) {
    for (ComputationCommitWorkRequest computationRequest : request.getRequestsList()) {
      for (WorkItemCommitRequest commit : computationRequest.getRequestsList()) {
        commitsReceived.put(commit.getWorkToken(), commit);
      }
    }
    return CommitWorkResponse.newBuilder().build();
  }

  @Override
  public Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request) {
    return Windmill.GetConfigResponse.newBuilder().build();
  }

  @Override
  public Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request) {
    for (Windmill.Exception exception : request.getExceptionsList()) {
      try {
        exceptions.put(exception);
      } catch (InterruptedException expected) {
      }
    }

    if (request.getExceptionsList().isEmpty() || expectedExceptionCount.getAndDecrement() > 0) {
      return Windmill.ReportStatsResponse.newBuilder().build();
    } else {
      return Windmill.ReportStatsResponse.newBuilder().setFailed(true).build();
    }
  }


  public void waitForEmptyWorkQueue() {
    while (!workToOffer.isEmpty()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException expected) {
      }
    }
  }

  public Map<Long, WorkItemCommitRequest> waitForAndGetCommits(int numCommits) {
    int maxTries = 10;
    while (maxTries-- > 0 && commitsReceived.size() < commitsRequested + numCommits) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException expected) {
      }
    }

    assertFalse(
        "Should have received commits after 10s, but only got " + commitsReceived,
        commitsReceived.size() < commitsRequested + numCommits);
    commitsRequested += numCommits;

    return commitsReceived;
  }

  public void setExpectedExceptionCount(int i) {
    expectedExceptionCount.getAndAdd(i);
  }

  public Windmill.Exception getException() throws InterruptedException {
    return exceptions.take();
  }
}
