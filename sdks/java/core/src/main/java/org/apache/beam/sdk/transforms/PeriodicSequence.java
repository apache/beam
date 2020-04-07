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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.*;

import java.util.List;
import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} which generates a sequence of timestamped elements at
 * given interval in runtime.
 *
 * <p>Receives a PCollection<List<Long>> where each element triggers the
 * generation of sequence and has following elements:
 * 0: first element timestamp
 * 1: last element timestamp
 * 2: interval
 *
 * <p>All elements that have timestamp in the past will be output right away.
 * Elements that have timestamp in the future will be delayed.
 *
 * <p>Transform will not output elements prior to target timestamp.
 * Transform can output elements at any time after target timestamp.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class PeriodicSequence
    extends PTransform<PCollection<List<Long>>, PCollection<Instant>> {

  private PeriodicSequence() {
  }

  public static PeriodicSequence create() {
    return new PeriodicSequence();
  }

  @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
  public static class OutputRangeTracker
      extends RestrictionTracker<OffsetRange, Long>
      implements Sizes.HasSize {
    private OffsetRange range;
    @Nullable
    private Long lastClaimedOffset = null;
    @Nullable
    private Long lastAttemptedOffset = null;

    public OutputRangeTracker(OffsetRange range) {
      this.range = checkNotNull(range);
      lastClaimedOffset = this.range.getFrom();
      lastAttemptedOffset = lastClaimedOffset;
    }

    @Override
    public OffsetRange currentRestriction() {
      return range;
    }

    @Override
    public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
      if (fractionOfRemainder != 0) {
        return null;
      }
      OffsetRange res = new OffsetRange(lastClaimedOffset, range.getTo());
      this.range = new OffsetRange(range.getFrom(), lastClaimedOffset);
      return SplitResult.of(range, res);
    }

    @Override
    public boolean tryClaim(Long i) {
      checkArgument(
          i > lastAttemptedOffset,
          "Trying to claim offset %s while last attempted was %s",
          i,
          lastAttemptedOffset);
      checkArgument(
          i >= range.getFrom(),
          "Trying to claim offset %s before start of the range %s", i, range);
      lastAttemptedOffset = i;
      if (i > range.getTo()) {
        return false;
      }
      lastClaimedOffset = i;
      return true;
    }

    @Override
    public void checkDone() throws IllegalStateException {
      checkState(
          lastAttemptedOffset >= range.getTo() - 1,
          "Last attempted offset was %s in range %s, claiming work in (%s, %s] was not attempted",
          lastAttemptedOffset,
          range,
          lastAttemptedOffset,
          range.getTo());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("range", range)
          .add("lastClaimedOffset", lastClaimedOffset)
          .add("lastAttemptedOffset", lastAttemptedOffset)
          .toString();
    }

    @Override
    public double getSize() {
      return Math.max(range.getTo() - lastAttemptedOffset, 0);
    }
  }

  private static class PeriodicSequenceFn extends DoFn<List<Long>, Instant> {
    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element List<Long> element) {
      String instruction =
          "Source elements should be List<Long> with:\n"
              + "  0: start timestamp\n"
              + "  1: stop timestamp\n"
              + "  3: period interval";
      checkArgument(element.size() >= 3, instruction);
      return new OffsetRange(element.get(0) - element.get(2), element.get(1));
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(
        @Restriction OffsetRange restriction) {
      return new OutputRangeTracker(restriction);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element List<Long> srcElement,
        OutputReceiver<Instant> out,
        RestrictionTracker<OffsetRange, Long> restrictionTracker) {

      OffsetRange restriction = restrictionTracker.currentRestriction();
      Long interval = srcElement.get(2);
      Long nextOutput = restriction.getFrom() + interval;

      boolean claimSuccess = true;

      while (claimSuccess && Instant.ofEpochMilli(nextOutput).isBeforeNow()) {
        claimSuccess = restrictionTracker.tryClaim(nextOutput);
        if (claimSuccess) {
          Instant output = Instant.ofEpochMilli(nextOutput);
          out.outputWithTimestamp(output, output);
          nextOutput = nextOutput + interval;
        }
      }

      ProcessContinuation continuation = ProcessContinuation.stop();
      if (claimSuccess) {
        Duration offset =
            new Duration(Instant.now(), Instant.ofEpochMilli(nextOutput));
        continuation = ProcessContinuation.resume().withResumeDelay(offset);
      }
      return continuation;
    }
  }

  @Override
  public PCollection<Instant> expand(PCollection<List<Long>> input) {
    return input.apply(ParDo.of(new PeriodicSequenceFn()));
  }
}
