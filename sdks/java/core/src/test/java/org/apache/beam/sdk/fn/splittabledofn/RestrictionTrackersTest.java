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
    private volatile boolean isBlocked;
    private volatile Progress currentProgress = REPORT_PROGRESS;
    public static final Progress REPORT_PROGRESS = Progress.from(2.0, 3.0);
    public static final Progress UPDATED_PROGRESS = Progress.from(4.0, 1.0);

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
      return currentProgress;
    }

    public void setProgress(Progress progress) {
      this.currentProgress = progress;
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

    public synchronized void setBlockTryClaim(boolean blockTryClaim) {
      this.blockTryClaim = blockTryClaim;
    }

    public synchronized void setBlockTrySplit(boolean blockTrySplit) {
      this.blockTrySplit = blockTrySplit;
    }

    public synchronized void releaseLock() {
      blockTrySplit = false;
      blockTryClaim = false;
      notifyAll();
    }

    /** Wait until RestrictionTracker becomes blocking or unblocking. */
    public void waitUntilBlocking(boolean blocking) throws InterruptedException {
      while (isBlocked != blocking) {
        Thread.sleep(1);
      }
    }
  }

  @Test
  public void testClaimObserversMaintainBacklogInterfaces() {
    RestrictionTracker hasSize =
        RestrictionTrackers.observe(new RestrictionTrackerWithProgress(), null);
    assertThat(hasSize, instanceOf(HasProgress.class));
  }

  @Test
  public void testClaimObserversProgressNonBlockingOnTryClaim() throws InterruptedException {
    RestrictionTrackerWithProgress withProgress = new RestrictionTrackerWithProgress(true, false);
    RestrictionTracker<Object, Object> tracker =
        RestrictionTrackers.observe(withProgress, new RestrictionTrackers.NoopClaimObserver<>());
    Thread blocking = new Thread(() -> tracker.tryClaim(new Object()));
    blocking.start();
    withProgress.waitUntilBlocking(true);
    // Times out while first tryClaim holds lock; returns NONE and sets needsProgressUpdate = true
    RestrictionTracker.Progress progress =
        ((RestrictionTrackers.RestrictionTrackerObserverWithProgress) tracker).getProgress(1);
    assertEquals(RestrictionTracker.Progress.NONE, progress);
    // When first tryClaim finishes, unlock() sees needsProgressUpdate == true and evaluates
    // REPORT_PROGRESS before releasing the lock.
    withProgress.releaseLock();
    withProgress.waitUntilBlocking(false);
    blocking.join();

    // Even if a second blocking tryClaim immediately grabs the lock before getProgress is called
    // again, getProgress(1) returns REPORT_PROGRESS (updated during first tryClaim's unlock).
    withProgress.setProgress(RestrictionTrackerWithProgress.UPDATED_PROGRESS);
    withProgress.setBlockTryClaim(true);
    Thread secondBlocking = new Thread(() -> tracker.tryClaim(new Object()));
    secondBlocking.start();
    withProgress.waitUntilBlocking(true);
    progress =
        ((RestrictionTrackers.RestrictionTrackerObserverWithProgress) tracker).getProgress(1);
    assertEquals(RestrictionTrackerWithProgress.REPORT_PROGRESS, progress);
    withProgress.releaseLock();
    withProgress.waitUntilBlocking(false);
    secondBlocking.join();
    progress = ((HasProgress) tracker).getProgress();
    assertEquals(RestrictionTrackerWithProgress.UPDATED_PROGRESS, progress);
  }

  @Test
  public void testClaimObserversProgressNonBlockingOnTrySplit() throws InterruptedException {
    RestrictionTrackerWithProgress withProgress = new RestrictionTrackerWithProgress(false, true);
    RestrictionTracker<Object, Object> tracker =
        RestrictionTrackers.observe(withProgress, new RestrictionTrackers.NoopClaimObserver<>());
    Thread blocking = new Thread(() -> tracker.trySplit(0.5));
    blocking.start();
    withProgress.waitUntilBlocking(true);
    // Times out while first trySplit holds lock; returns NONE and sets needsProgressUpdate = true
    RestrictionTracker.Progress progress =
        ((RestrictionTrackers.RestrictionTrackerObserverWithProgress) tracker).getProgress(1);
    assertEquals(RestrictionTracker.Progress.NONE, progress);
    // When first trySplit finishes, unlock() sees needsProgressUpdate == true and evaluates
    // REPORT_PROGRESS before releasing the lock.
    withProgress.releaseLock();
    withProgress.waitUntilBlocking(false);
    blocking.join();

    // Even if a second blocking trySplit immediately grabs the lock before getProgress is called
    // again, getProgress(1) returns REPORT_PROGRESS (updated during first trySplit's unlock).
    withProgress.setProgress(RestrictionTrackerWithProgress.UPDATED_PROGRESS);
    withProgress.setBlockTrySplit(true);
    Thread secondBlocking = new Thread(() -> tracker.trySplit(0.5));
    secondBlocking.start();
    withProgress.waitUntilBlocking(true);
    progress =
        ((RestrictionTrackers.RestrictionTrackerObserverWithProgress) tracker).getProgress(1);
    assertEquals(RestrictionTrackerWithProgress.REPORT_PROGRESS, progress);
    withProgress.releaseLock();
    withProgress.waitUntilBlocking(false);
    secondBlocking.join();
    progress = ((HasProgress) tracker).getProgress();
    assertEquals(RestrictionTrackerWithProgress.UPDATED_PROGRESS, progress);
  }
}
