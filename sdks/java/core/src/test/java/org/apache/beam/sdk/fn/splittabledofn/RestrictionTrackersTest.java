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
package org.apache.beam.sdk.fn.splittabledofn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.fn.splittabledofn.RestrictionTrackers.ClaimObserver;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RestrictionTrackers}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class RestrictionTrackersTest {
  @Rule public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

  @Test
  public void testObservingClaims() {
    RestrictionTracker<String, String> observedTracker =
        new RestrictionTracker() {

          @Override
          public boolean tryClaim(Object position) {
            return "goodClaim".equals(position);
          }

          @Override
          public Object currentRestriction() {
            throw new UnsupportedOperationException();
          }

          @Override
          public SplitResult<Object> trySplit(double fractionOfRemainder) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void checkDone() throws IllegalStateException {
            throw new UnsupportedOperationException();
          }

          @Override
          public IsBounded isBounded() {
            return IsBounded.BOUNDED;
          }
        };

    List<String> positionsObserved = new ArrayList<>();
    ClaimObserver<String> observer =
        new ClaimObserver<String>() {

          @Override
          public void onClaimed(String position) {
            positionsObserved.add(position);
            assertEquals("goodClaim", position);
          }

          @Override
          public void onClaimFailed(String position) {
            positionsObserved.add(position);
          }
        };

    RestrictionTracker<String, String> observingTracker =
        RestrictionTrackers.observe(observedTracker, observer);
    observingTracker.tryClaim("goodClaim");
    observingTracker.tryClaim("badClaim");

    assertThat(positionsObserved, contains("goodClaim", "badClaim"));
  }

  private static class RestrictionTrackerWithProgress extends RestrictionTracker<Object, Object>
      implements HasProgress {
    private boolean blockTryClaim;
    private boolean blockTrySplit;
    private boolean isBlocked;
    public static Progress REPORT_PROGRESS = Progress.from(2.0, 3.0);

    public RestrictionTrackerWithProgress() {
      this(false, false);
    }

    public RestrictionTrackerWithProgress(boolean blockTryClaim, boolean blockTrySplit) {
      this.blockTryClaim = blockTryClaim;
      this.blockTrySplit = blockTrySplit;
      this.isBlocked = false;
    }

    @Override
    public Progress getProgress() {
      return REPORT_PROGRESS;
    }

    @Override
    public synchronized boolean tryClaim(Object position) {
      while (blockTryClaim) {
        isBlocked = true;
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      isBlocked = false;
      return false;
    }

    @Override
    public Object currentRestriction() {
      return null;
    }

    @Override
    public synchronized SplitResult<Object> trySplit(double fractionOfRemainder) {
      while (blockTrySplit) {
        isBlocked = true;
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      isBlocked = false;
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }

    public synchronized void releaseLock() {
      blockTrySplit = false;
      blockTryClaim = false;
      notifyAll();
    }

    public boolean isBlocked() {
      return isBlocked;
    }
  }

  @Test
  public void testClaimObserversMaintainBacklogInterfaces() {
    RestrictionTracker hasSize =
        RestrictionTrackers.observe(new RestrictionTrackerWithProgress(), null);
    assertThat(hasSize, instanceOf(HasProgress.class));
  }

  private void testBlocking(String testCase) throws InterruptedException {
    RestrictionTrackerWithProgress withProgress;
    if ("tryClaim".equals(testCase)) {
      withProgress = new RestrictionTrackerWithProgress(true, false);
    } else if ("trySplit".equals(testCase)) {
      withProgress = new RestrictionTrackerWithProgress(false, true);
    } else {
      throw new IllegalArgumentException("unknown test case " + testCase);
    }
    RestrictionTracker<Object, Object> tracker =
        RestrictionTrackers.observe(withProgress, new RestrictionTrackers.NoopClaimObserver<>());
    Runnable runnable;
    if ("tryClaim".equals(testCase)) {
      runnable = () -> tracker.tryClaim(new Object());
    } else {
      runnable = () -> tracker.trySplit(0.5);
    }
    Thread blocking = new Thread(runnable);
    blocking.start();
    while (!withProgress.isBlocked()) {
      Thread.sleep(1);
    }
    RestrictionTracker.Progress progress = ((HasProgress) tracker).getProgress();
    assertEquals(RestrictionTracker.Progress.NONE, progress);
    withProgress.releaseLock();
    while (withProgress.isBlocked()) {
      Thread.sleep(1);
    }
    progress = ((HasProgress) tracker).getProgress();
    assertEquals(RestrictionTrackerWithProgress.REPORT_PROGRESS, progress);
  }

  @Test
  public void testClaimObserversProgressNonBlockingOnTryClaim() throws InterruptedException {
    testBlocking("tryClaim");
  }

  @Test
  public void testClaimObserversProgressNonBlockingOnTrySplit() throws InterruptedException {
    testBlocking("trySplit");
  }
}
