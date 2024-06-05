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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table.Cell;

public class FnApiTimerBundleTracker<K> {
  private final Supplier<ByteString> encodedCurrentKeySupplier;
  private final Supplier<ByteString> encodedCurrentWindowSupplier;
  private final Table<ByteString, ByteString, Modifications<K>> timerModifications;

  @AutoValue
  public abstract static class TimerInfo<K> {
    public abstract Timer<K> getTimer();

    public abstract String getTimerFamilyOrId();

    public abstract TimeDomain getTimeDomain();

    public static <K> TimerInfo<K> of(
        Timer<K> timer, String timerFamilyOrId, TimeDomain timeDomain) {
      return new AutoValue_FnApiTimerBundleTracker_TimerInfo<>(timer, timerFamilyOrId, timeDomain);
    }
  }

  @AutoValue
  public abstract static class Modifications<K> {
    public abstract NavigableSet<TimerInfo<K>> getModifiedEventTimersOrdered();

    public abstract NavigableSet<TimerInfo<K>> getModifiedProcessingTimersOrdered();

    public abstract NavigableSet<TimerInfo<K>> getModifiedSynchronizedProcessingTimersOrdered();

    public NavigableSet<TimerInfo<K>> getModifiedTimersOrdered(TimeDomain timeDomain) {
      switch (timeDomain) {
        case EVENT_TIME:
          return getModifiedEventTimersOrdered();
        case PROCESSING_TIME:
          return getModifiedProcessingTimersOrdered();
        case SYNCHRONIZED_PROCESSING_TIME:
          return getModifiedSynchronizedProcessingTimersOrdered();
        default:
          throw new RuntimeException("Unexpected time domain " + timeDomain);
      }
    }

    public abstract Table<String, String, Timer<K>> getModifiedTimerIds();

    @SuppressWarnings({"nullness"})
    static <K> Modifications<K> create() {
      Comparator<TimeDomain> timeDomainComparator =
          (td1, td2) -> {
            // We prioritize processing-time timers,as those tend to be more latency sensitive.
            if (td1 == TimeDomain.PROCESSING_TIME && td2 == TimeDomain.EVENT_TIME) {
              return -1;
            } else if (td1 == TimeDomain.EVENT_TIME && td2 == TimeDomain.PROCESSING_TIME) {
              return 1;
            } else {
              return td1.compareTo(td2);
            }
          };

      // We don't compare userKey or window, as all timers in the TreeSet already have the same
      // key/window.
      Comparator<TimerInfo<K>> comparator =
          (o1, o2) -> {
            ComparisonChain chain =
                ComparisonChain.start()
                    .compare(o1.getTimeDomain(), o2.getTimeDomain(), timeDomainComparator)
                    .compareTrueFirst(o1.getTimer().getClearBit(), o2.getTimer().getClearBit())
                    .compare(o1.getTimer().getFireTimestamp(), o2.getTimer().getFireTimestamp())
                    .compare(o1.getTimer().getHoldTimestamp(), o2.getTimer().getHoldTimestamp())
                    .compare(
                        o1.getTimer().getDynamicTimerTag(), o2.getTimer().getDynamicTimerTag());
            return chain.result();
          };

      return new AutoValue_FnApiTimerBundleTracker_Modifications<>(
          Sets.newTreeSet(comparator),
          Sets.newTreeSet(comparator),
          Sets.newTreeSet(comparator),
          HashBasedTable.create());
    }
  }

  public FnApiTimerBundleTracker(
      Coder<K> keyCoder,
      Coder<BoundedWindow> windowCoder,
      Supplier<K> currentKeySupplier,
      Supplier<BoundedWindow> currentWindowSupplier) {
    timerModifications = HashBasedTable.create();
    this.encodedCurrentKeySupplier =
        memoizeFunction(
            currentKeySupplier,
            key -> {
              checkState(
                  keyCoder != null, "Accessing state in unkeyed context, no key coder available");

              ByteStringOutputStream encodedKeyOut = new ByteStringOutputStream();
              try {
                keyCoder.encode(key, encodedKeyOut, Coder.Context.NESTED);
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
              return encodedKeyOut.toByteString();
            });
    this.encodedCurrentWindowSupplier =
        memoizeFunction(
            currentWindowSupplier,
            window -> {
              ByteStringOutputStream encodedWindowOut = new ByteStringOutputStream();
              try {
                windowCoder.encode(window, encodedWindowOut);
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
              return encodedWindowOut.toByteString();
            });
  }

  public void reset() {
    timerModifications.clear();
  }

  public void timerModified(String timerFamilyOrId, TimeDomain timeDomain, Timer<K> timer) {
    ByteString keyString = encodedCurrentKeySupplier.get();
    ByteString windowString = encodedCurrentWindowSupplier.get();
    @Nullable Modifications<K> modifications = timerModifications.get(keyString, windowString);
    if (modifications == null) {
      modifications = Modifications.create();
      timerModifications.put(keyString, windowString, modifications);
    }
    if (!timer.getClearBit()) {
      modifications
          .getModifiedTimersOrdered(timeDomain)
          .add(TimerInfo.of(timer, timerFamilyOrId, timeDomain));
    }
    modifications.getModifiedTimerIds().put(timerFamilyOrId, timer.getDynamicTimerTag(), timer);
  }

  public Modifications<K> getBundleModifications() {
    ByteString keyString = encodedCurrentKeySupplier.get();
    ByteString windowString = encodedCurrentWindowSupplier.get();
    @Nullable Modifications<K> modifications = timerModifications.get(keyString, windowString);
    if (modifications == null) {
      modifications = Modifications.create();
      timerModifications.put(keyString, windowString, modifications);
    }
    return modifications;
  }

  public void outputTimers(
      Function<String, FnDataReceiver<Timer<?>>> getTimersReceiverFromTimerIdFn) {
    for (Cell<ByteString, ByteString, Modifications<K>> cell : timerModifications.cellSet()) {
      Modifications<K> modifications = cell.getValue();
      if (modifications != null) {
        for (Cell<String, String, Timer<K>> timerCell :
            modifications.getModifiedTimerIds().cellSet()) {
          String timerFamilyOrId = timerCell.getRowKey();
          Timer<K> timer = timerCell.getValue();
          try {
            if (timerFamilyOrId != null && timer != null) {
              getTimersReceiverFromTimerIdFn.apply(timerFamilyOrId).accept(timer);
            }
          } catch (Throwable t) {
            throw UserCodeException.wrap(t);
          }
        }
      }
    }
  }

  private static <ArgT, ResultT> Supplier<ResultT> memoizeFunction(
      Supplier<ArgT> arg, Function<ArgT, ResultT> f) {
    return new Supplier<ResultT>() {
      private @Nullable ArgT memoizedArg = null;
      private @Nullable ResultT memoizedResult = null;

      @Override
      public ResultT get() {
        ArgT currentArg = arg.get();
        if (memoizedArg == null || currentArg != memoizedArg) {
          this.memoizedArg = currentArg;
          memoizedResult = f.apply(currentArg);
        }
        if (memoizedResult != null) {
          return memoizedResult;
        } else {
          throw new RuntimeException("Unexpected null result.");
        }
      }
    };
  }
}
