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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.internal.wire.Committer;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(JUnit4.class)
public class BlockingCommitterImplTest {

  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  abstract static class FakeCommitter extends FakeApiService implements Committer {}

  @Spy private FakeCommitter fakeCommitter;

  private BlockingCommitter committer;

  @Before
  public void setUp() {
    fakeCommitter.startAsync().awaitRunning();
    committer = new BlockingCommitterImpl(fakeCommitter);
  }

  @Test
  public void commit() {
    doReturn(ApiFutures.immediateFuture(null)).when(fakeCommitter).commitOffset(Offset.of(42));
    committer.commitOffset(Offset.of(42));
  }

  @Test
  public void close() throws Exception {
    committer.close();
    verify(fakeCommitter).stopAsync();
    verify(fakeCommitter).awaitTerminated(1, TimeUnit.MINUTES);
  }
}
