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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EvenGetWorkBudgetDistributorTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private static GetWorkBudgetSpender createGetWorkBudgetOwner() {
    // Lambdas are final and cannot be spied.
    return spy(
        new GetWorkBudgetSpender() {

          @Override
          public void setBudget(long items, long bytes) {}
        });
  }

  @Test
  public void testDistributeBudget_doesNothingWhenPassedInStreamsEmpty() {
    GetWorkBudgetDistributors.distributeEvenly()
        .distributeBudget(
            ImmutableList.of(), GetWorkBudget.builder().setItems(10L).setBytes(10L).build());
  }

  @Test
  public void testDistributeBudget_doesNothingWithNoBudget() {
    GetWorkBudgetSpender getWorkBudgetSpender = spy(createGetWorkBudgetOwner());
    GetWorkBudgetDistributors.distributeEvenly()
        .distributeBudget(ImmutableList.of(getWorkBudgetSpender), GetWorkBudget.noBudget());
    verifyNoInteractions(getWorkBudgetSpender);
  }

  @Test
  public void testDistributeBudget_distributesBudgetEvenlyIfPossible() {
    long totalItemsAndBytes = 10L;
    List<GetWorkBudgetSpender> streams = new ArrayList<>();
    for (int i = 0; i < totalItemsAndBytes; i++) {
      streams.add(spy(createGetWorkBudgetOwner()));
    }
    GetWorkBudgetDistributors.distributeEvenly()
        .distributeBudget(
            ImmutableList.copyOf(streams),
            GetWorkBudget.builder()
                .setItems(totalItemsAndBytes)
                .setBytes(totalItemsAndBytes)
                .build());

    long itemsAndBytesPerStream = totalItemsAndBytes / streams.size();
    streams.forEach(
        stream ->
            verify(stream, times(1))
                .setBudget(eq(itemsAndBytesPerStream), eq(itemsAndBytesPerStream)));
  }

  @Test
  public void testDistributeBudget_distributesFairlyWhenNotEven() {
    long totalItemsAndBytes = 10L;
    List<GetWorkBudgetSpender> streams = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      streams.add(spy(createGetWorkBudgetOwner()));
    }
    GetWorkBudgetDistributors.distributeEvenly()
        .distributeBudget(
            ImmutableList.copyOf(streams),
            GetWorkBudget.builder()
                .setItems(totalItemsAndBytes)
                .setBytes(totalItemsAndBytes)
                .build());

    long itemsAndBytesPerStream = (long) Math.ceil(totalItemsAndBytes / (streams.size() * 1.0));
    streams.forEach(
        stream ->
            verify(stream, times(1))
                .setBudget(eq(itemsAndBytesPerStream), eq(itemsAndBytesPerStream)));
  }

  @Test
  public void testDistributeBudget_distributesBudgetEvenly() {
    long totalItemsAndBytes = 10L;
    List<GetWorkBudgetSpender> streams = new ArrayList<>();
    for (int i = 0; i < totalItemsAndBytes; i++) {
      streams.add(spy(createGetWorkBudgetOwner()));
    }

    GetWorkBudgetDistributors.distributeEvenly()
        .distributeBudget(
            ImmutableList.copyOf(streams),
            GetWorkBudget.builder()
                .setItems(totalItemsAndBytes)
                .setBytes(totalItemsAndBytes)
                .build());

    long itemsAndBytesPerStream = totalItemsAndBytes / streams.size();
    streams.forEach(
        stream ->
            verify(stream, times(1))
                .setBudget(eq(itemsAndBytesPerStream), eq(itemsAndBytesPerStream)));
  }
}
