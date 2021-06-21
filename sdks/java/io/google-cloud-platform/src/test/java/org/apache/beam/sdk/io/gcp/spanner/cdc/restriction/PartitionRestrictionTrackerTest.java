package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.IsBounded;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class PartitionRestrictionTrackerTest {

  private PartitionRestrictionTracker tracker;
  private PartitionRestriction restriction;

  @Before
  public void setUp() {
    final Timestamp startTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 20);
    restriction = new PartitionRestriction(startTimestamp, null, null);
    tracker = new PartitionRestrictionTracker(restriction);
  }

  @Test
  public void testTryClaimModeTransitions() {
    final TryClaimTestScenario runner = new TryClaimTestScenario(
        tracker, Timestamp.ofTimeSecondsAndNanos(10L, 20));

    runner.from(null).to(QUERY_CHANGE_STREAM).assertSuccess();
    runner.from(null).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(null).to(FINISH_PARTITION).assertError();
    runner.from(null).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(null).to(DELETE_PARTITION).assertError();
    runner.from(null).to(DONE).assertError();

    runner.from(QUERY_CHANGE_STREAM).to(QUERY_CHANGE_STREAM).assertSuccess();
    runner.from(QUERY_CHANGE_STREAM).to(WAIT_FOR_CHILD_PARTITIONS).assertSuccess();
    runner.from(QUERY_CHANGE_STREAM).to(FINISH_PARTITION).assertSuccess();
    runner.from(QUERY_CHANGE_STREAM).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(QUERY_CHANGE_STREAM).to(DELETE_PARTITION).assertError();
    runner.from(QUERY_CHANGE_STREAM).to(DONE).assertError();

    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(QUERY_CHANGE_STREAM).assertError();
    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(FINISH_PARTITION).assertSuccess();
    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(DELETE_PARTITION).assertError();
    runner.from(WAIT_FOR_CHILD_PARTITIONS).to(DONE).assertError();

    runner.from(FINISH_PARTITION).to(QUERY_CHANGE_STREAM).assertError();
    runner.from(FINISH_PARTITION).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(FINISH_PARTITION).to(FINISH_PARTITION).assertError();
    runner.from(FINISH_PARTITION).to(WAIT_FOR_PARENT_PARTITIONS).assertSuccess();
    runner.from(FINISH_PARTITION).to(DELETE_PARTITION).assertError();
    runner.from(FINISH_PARTITION).to(DONE).assertError();

    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(QUERY_CHANGE_STREAM).assertError();
    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(FINISH_PARTITION).assertError();
    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(DELETE_PARTITION).assertSuccess();
    runner.from(WAIT_FOR_PARENT_PARTITIONS).to(DONE).assertError();

    runner.from(DELETE_PARTITION).to(QUERY_CHANGE_STREAM).assertError();
    runner.from(DELETE_PARTITION).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(DELETE_PARTITION).to(FINISH_PARTITION).assertError();
    runner.from(DELETE_PARTITION).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(DELETE_PARTITION).to(DELETE_PARTITION).assertError();
    runner.from(DELETE_PARTITION).to(DONE).assertSuccess();

    runner.from(DONE).to(QUERY_CHANGE_STREAM).assertError();
    runner.from(DONE).to(WAIT_FOR_CHILD_PARTITIONS).assertError();
    runner.from(DONE).to(FINISH_PARTITION).assertError();
    runner.from(DONE).to(WAIT_FOR_PARENT_PARTITIONS).assertError();
    runner.from(DONE).to(DELETE_PARTITION).assertError();
    runner.from(DONE).to(DONE).assertError();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimTimestampInThePastThrowsAnError() {
    tracker.tryClaim(PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(10L, 19)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWithZeroChildPartitionsToWaitForThrowsAnError() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(QUERY_CHANGE_STREAM);
    tracker.setLastClaimedChildPartitionsToWaitFor(null);

    tracker.tryClaim(PartitionPosition.waitForChildPartitions(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTryClaimWithNegativeChildPartitionsToWaitForThrowsAnError() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(QUERY_CHANGE_STREAM);
    tracker.setLastClaimedChildPartitionsToWaitFor(null);

    tracker.tryClaim(PartitionPosition.waitForChildPartitions(-1));
  }

  @Test
  public void testTrySplit() {
    assertNull(tracker.trySplit(1D));
  }

  @Test
  public void testCheckDoneWithSomeTimestampAndDoneMode() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(DONE);

    // No exceptions should be raised, which indicates success
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedTimestamp() {
    tracker.checkDone();
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithNullLastClaimedMode() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));

    tracker.checkDone();
  }

  // FIXME: Either remove this test or fix the check done
  @Ignore
  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsPartitionQuery() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(QUERY_CHANGE_STREAM);

    tracker.checkDone();
  }

  // FIXME: Either remove this test or fix the check done
  @Ignore
  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsWaitForChildren() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(WAIT_FOR_CHILD_PARTITIONS);

    tracker.checkDone();
  }

  // FIXME: Either remove this test or fix the check done
  @Ignore
  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsWaitForParents() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(WAIT_FOR_PARENT_PARTITIONS);

    tracker.checkDone();
  }

  // FIXME: Either remove this test or fix the check done
  @Ignore
  @Test(expected = IllegalStateException.class)
  public void testCheckDoneWithLastClaimedModeAsDeletePartition() {
    tracker.setLastClaimedTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20));
    tracker.setLastClaimedMode(DELETE_PARTITION);

    tracker.checkDone();
  }

  @Test
  public void testIsBounded() {
    assertEquals(IsBounded.UNBOUNDED, tracker.isBounded());
  }

  private static class TryClaimTestScenario {

    private final PartitionRestrictionTracker restrictionTracker;
    private final Timestamp defaultTimestamp;
    private final Long defaultChildPartitionsToWaitFor;
    public PartitionMode fromMode;
    public PartitionMode toMode;

    private TryClaimTestScenario(
        PartitionRestrictionTracker restrictionTracker,
        Timestamp defaultTimestamp) {
      this.restrictionTracker = restrictionTracker;
      this.defaultTimestamp = defaultTimestamp;
      this.defaultChildPartitionsToWaitFor = 0L;
    }

    public TryClaimTestScenario from(PartitionMode from) {
      this.fromMode = from;
      return this;
    }

    public TryClaimTestScenario to(PartitionMode to) {
      this.toMode = to;
      return this;
    }

    public void assertSuccess() {
      run(false);
    }

    public void assertError() {
      run(true);
    }

    private void run(boolean errorExpected) {
      restrictionTracker.setLastClaimedTimestamp(defaultTimestamp);
      restrictionTracker.setLastClaimedMode(fromMode);
      restrictionTracker.setLastClaimedChildPartitionsToWaitFor(defaultChildPartitionsToWaitFor);
      final Consumer<PartitionPosition> assertFn = errorExpected ?
          (PartitionPosition position) -> {
            assertThrows(IllegalArgumentException.class, () -> restrictionTracker.tryClaim(position));
          } :
          (PartitionPosition position) -> {
            assertTrue(restrictionTracker.tryClaim(position));
            assertEquals(
                toMode,
                restrictionTracker.currentRestriction().getMode()
            );
            assertEquals(
                position.getTimestamp().orElse(defaultTimestamp),
                restrictionTracker.currentRestriction().getStartTimestamp()
            );
            assertEquals(
                position.getChildPartitionsToWaitFor().orElse(defaultChildPartitionsToWaitFor),
                restrictionTracker.currentRestriction().getChildPartitionsToWaitFor()
            );
          };

      switch (toMode) {
        case QUERY_CHANGE_STREAM:
          assertFn.accept(PartitionPosition.queryChangeStream(Timestamp.ofTimeSecondsAndNanos(100L, 200)));
          break;
        case WAIT_FOR_CHILD_PARTITIONS:
          assertFn.accept(PartitionPosition.waitForChildPartitions(10L));
          break;
        case FINISH_PARTITION:
          assertFn.accept(PartitionPosition.finishPartition());
          break;
        case WAIT_FOR_PARENT_PARTITIONS:
          assertFn.accept(PartitionPosition.waitForParentPartitions());
          break;
        case DELETE_PARTITION:
          assertFn.accept(PartitionPosition.deletePartition());
          break;
        case DONE:
          assertFn.accept(PartitionPosition.done());
          break;
        default:
          throw new IllegalArgumentException("Unknown mode " + toMode);
      }
    }
  }
}
