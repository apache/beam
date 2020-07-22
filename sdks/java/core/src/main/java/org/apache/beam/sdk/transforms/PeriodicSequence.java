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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link PTransform} which generates a sequence of timestamped elements at given runtime
 * intervals.
 *
 * <p>Transform assigns each element some timestamp and will only output element when worker clock
 * reach given timestamp. Transform will not output elements prior to target time. Transform can
 * output elements at any time after target time.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class PeriodicSequence
    extends PTransform<PCollection<PeriodicSequence.SequenceDefinition>, PCollection<Instant>> {

  @DefaultSchema(JavaFieldSchema.class)
  public static class SequenceDefinition {
    public Instant first;
    public Instant last;
    public Long durationMilliSec;

    public SequenceDefinition() {}

    public SequenceDefinition(Instant first, Instant last, Duration duration) {
      this.first = first;
      this.last = last;
      this.durationMilliSec = duration.getMillis();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }

      SequenceDefinition src = (SequenceDefinition) obj;
      return src.first.equals(this.first)
          && src.last.equals(this.last)
          && src.durationMilliSec.equals(this.durationMilliSec);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(first, last, durationMilliSec);
      return result;
    }
  }

  private PeriodicSequence() {}

  public static PeriodicSequence create() {
    return new PeriodicSequence();
  }

  @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
  public static class OutputRangeTracker extends RestrictionTracker<OffsetRange, Long>
      implements RestrictionTracker.HasProgress {
    private OffsetRange range;
    private @Nullable Long lastClaimedOffset = null;
    private @Nullable Long lastAttemptedOffset = null;

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
          i >= range.getFrom(), "Trying to claim offset %s before start of the range %s", i, range);
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
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
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
    public Progress getProgress() {
      double workRemaining = Math.max(range.getTo() - lastAttemptedOffset, 0);
      return Progress.from(range.getTo() - range.getFrom() - workRemaining, workRemaining);
    }
  }

  private static class PeriodicSequenceFn extends DoFn<SequenceDefinition, Instant> {
    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element SequenceDefinition element) {
      return new OffsetRange(
          element.first.getMillis() - element.durationMilliSec, element.last.getMillis());
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OutputRangeTracker(restriction);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element SequenceDefinition srcElement,
        OutputReceiver<Instant> out,
        RestrictionTracker<OffsetRange, Long> restrictionTracker) {

      OffsetRange restriction = restrictionTracker.currentRestriction();
      Long interval = srcElement.durationMilliSec;
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
        Duration offset = new Duration(Instant.now(), Instant.ofEpochMilli(nextOutput));
        continuation = ProcessContinuation.resume().withResumeDelay(offset);
      }
      return continuation;
    }
  }

  @Override
  public PCollection<Instant> expand(PCollection<SequenceDefinition> input) {
    return input.apply(ParDo.of(new PeriodicSequenceFn()));
  }
}
