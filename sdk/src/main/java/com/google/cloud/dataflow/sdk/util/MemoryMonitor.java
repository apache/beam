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

package com.google.cloud.dataflow.sdk.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * A runnable which monitors a server for GC thrashing.
 *
 * <p>Note: Only one instance of this should be initialized per server and
 * it should be done when the server starts running.
 *
 * <p>This runnable works as follows:
 * <ul>
 * <li> It wakes up periodically and determines how much time was spend on garbage
 *      collection since the last time it woke up.
 * <li> If the time spent in garbage collection in the last period of time
 *      exceeds a certain threshold, that period is marked as "being in GC thrashing"
 * <li> It keeps track of the GC thrashing status of the last few periods.
 * <li> Every time the runnable's thread wakes up, it computes the ratio
 *      {@code (# monitored periods in GC thrashing) / (# monitored periods)}.
 * <li> If this ratio exceeds a certain threshold, it is assumed that the server
 *      is in GC thrashing.
 * <li> It can also shutdown the current jvm runtime when a threshold of consecutive gc
 *      thrashing count is met. A heap dump is made before shutdown.
 * </ul>
 */
public class MemoryMonitor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitor.class);

  /** Directory to hold heap dumps if not overridden. */
  private static final String DEFAULT_LOGGING_DIR = "dataflow/logs";

  /**
   * Amount of time (in ms) this thread must sleep
   * between two consecutive iterations.
   */
  public static final long DEFAULT_SLEEP_TIME_MILLIS = 15 * 1000; // 15 sec.

  /**
   * The number of periods to take into account when determining
   * if the server is in GC thrashing.
   */
  private static final int NUM_MONITORED_PERIODS = 4; // ie 1 min's worth.

  /**
   * The GC thrashing threshold (0.00 - 100.00) for every period. If
   * the time spent on garbage collection in one period exceeds this
   * threshold, that period is considered to be in GC thrashing.
   */
  private static final double GC_THRASHING_PERCENTAGE_PER_PERIOD = 50.0;

  /**
   * The <code>(# monitored periods in GC thrashing) / (# monitored
   * periods)</code> threshold after which the server is considered to be in
   * GC thrashing, expressed as a percentage.
   */
  private static final double GC_THRASHING_PERCENTAGE_PER_SERVER = 60.0;

  /**
   * The amount of memory (in bytes) we should pre-allocate, in order
   * to be able to dump the heap.
   *
   * Since the server is in GC thrashing when we try to dump the heap, we
   * might not be able to successfully do it. However, if we pre-allocate a
   * big enough block of memory and "release" it right before trying to dump
   * the heap, the pre-allocated block of memory will get GCed, and the heap
   * dump might succeed.
   */
  private static final int HEAP_DUMP_RESERVED_BYTES = 10 << 20; // 10MB

  /**
   * Shutdown the current JVM instance after given consecutive gc thrashing
   * periods are detected. This offers an opportunity to fast kill a JVM server
   * if it is about to enter a long lasting gc thrashing state, which is almost
   * never a desired behavior for a healthy server. 0 to disable.
   */
  private static final int DEFAULT_SHUT_DOWN_AFTER_NUM_GCTHRASHING = 8; // ie 2 min's worth.

  /**
   * Delay between logging the current memory state.
   */
  private static final int NORMAL_LOGGING_PERIOD_MILLIS = 5 * 60 * 1000; // 5 min.

  /**
   * Abstract interface for providing GC stats (for testing).
   */
  public interface GCStatsProvider {
    /**
     * Return the total milliseconds spent in GC since JVM was started.
     */
    long totalGCTimeMilliseconds();
  }

  /**
   * True system GC stats.
   */
  private static class SystemGCStatsProvider implements GCStatsProvider {
    @Override
    public long totalGCTimeMilliseconds() {
      long inGC = 0;
      for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
        inGC += gc.getCollectionTime();
      }
      return inGC;
    }
  }

  /** Where to get GC stats. */
  private final GCStatsProvider gcStatsProvider;

  /** Actual sleep time, in milliseconds, for main monitor. */
  private final long sleepTimeMillis;

  /** Actual number of cycles before shutting down VM. */
  private final int shutDownAfterNumGCThrashing;

  /**
   * The state of the periods that are taken into account when
   * deciding if the server is in GC thrashing.
   */
  private final Queue<Boolean> periodIsThrashing = new ArrayDeque<>();

  /**
   * Keeps track of the time the server spent in GC since it started running.
   */
  private long timeInGC = 0;

  /**
   * A reserved block of memory, needed to dump the heap. Dumping the heap
   * requires memory. However, since we try to do it when the server is in
   * GC thrashing, no memory is available and dumpHeap() brings the server
   * down. If we pre-allocate a block of memory though, and "release" it
   * right before dumping the heap, this block of memory will be
   * garbage collected, thus giving dumpHeap() enough space to dump the heap.
   */
  @SuppressWarnings("unused")
  private byte[] reservedForDumpingHeap = new byte[HEAP_DUMP_RESERVED_BYTES];

  private final AtomicBoolean isThrashing = new AtomicBoolean(false);

  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private final AtomicDouble lastMeasuredGCPercentage = new AtomicDouble(0.0);
  private final AtomicDouble maxGCPercentage = new AtomicDouble(0.0);
  private final AtomicInteger numPushbacks = new AtomicInteger(0);

  /** Wait point for threads in pushback waiting for gc thrashing to pass. */
  private final Object waitingForResources = new Object();

  public MemoryMonitor() {
    gcStatsProvider = new SystemGCStatsProvider();
    sleepTimeMillis = DEFAULT_SLEEP_TIME_MILLIS;
    shutDownAfterNumGCThrashing = DEFAULT_SHUT_DOWN_AFTER_NUM_GCTHRASHING;
  }

  /**
   * For testing only: Construct memory monitor which takes GC stats from given provider
   * and uses given sleep time and shutdown threshold.
   */
  @VisibleForTesting
  public MemoryMonitor(
      GCStatsProvider gcStatsProvider, long sleepTimeMillis, int shutDownAfterNumGCThrashing) {
    this.gcStatsProvider = gcStatsProvider;
    this.sleepTimeMillis = sleepTimeMillis;
    this.shutDownAfterNumGCThrashing = shutDownAfterNumGCThrashing;
  }

  /**
   * Check if we've observed high gc workload in sufficient
   * sample periods to justify classifying the server as in gc thrashing.
   */
  private void updateIsThrashing() {
    // have we monitored enough periods?
    if (periodIsThrashing.size() < NUM_MONITORED_PERIODS) {
      setIsThrashing(false);
      return;
    }

    // count the number of periods in GC thrashing
    int numPeriodsInGCThrashing = 0;
    for (Boolean state : periodIsThrashing) {
      numPeriodsInGCThrashing += (state ? 1 : 0);
    }

    // Did we have too many periods in GC thrashing?
    boolean serverInGcThrashing = (numPeriodsInGCThrashing * 100
        >= periodIsThrashing.size() * GC_THRASHING_PERCENTAGE_PER_SERVER);
    setIsThrashing(serverInGcThrashing);
  }

  /**
   * Set the thrashing state.
   */
  private void setIsThrashing(boolean serverInGcThrashing) {
    synchronized (waitingForResources) {
      boolean prev = isThrashing.getAndSet(serverInGcThrashing);
      if (prev && !serverInGcThrashing) {
        waitingForResources.notifyAll();
      }
    }
  }

  /**
   * Determines if too much time was spent on garbage collection in the last
   * period of time.
   *
   * @param now The current time.
   * @param lastTimeWokeUp The last time this thread woke up.
   *
   * @return The state of the last period of time.
   */
  private boolean wasLastPeriodInGCThrashing(long now, long lastTimeWokeUp) {
    // Find out how much time was spent on garbage collection
    // since the start of the server.  This queries the set of garbage collectors for
    // how long each one has spent doing GC.
    long inGC = gcStatsProvider.totalGCTimeMilliseconds();

    // Compare the amount of time spent in GC thrashing to the given threshold;
    // if config.getSleepTimeMillis() is equal to 0 (should happen in tests only),
    // then we compare percentage-per-period to 100%
    double gcPercentage = (inGC - timeInGC) * 100 / (now - lastTimeWokeUp);

    lastMeasuredGCPercentage.set(gcPercentage);
    maxGCPercentage.set(Math.max(maxGCPercentage.get(), gcPercentage));
    timeInGC = inGC;

    return gcPercentage > GC_THRASHING_PERCENTAGE_PER_PERIOD;
  }

  /**
   * Updates the data we monitor.
   *
   * @param now The current time.
   * @param lastTimeWokeUp The last time this thread woke up.
   */
  private void updateData(long now, long lastTimeWokeUp) {
    // remove data that's no longer relevant
    int numIntervals = NUM_MONITORED_PERIODS;
    while (periodIsThrashing.size() >= numIntervals) {
      periodIsThrashing.poll();
    }
    // store the state of the last period
    boolean wasThrashing = wasLastPeriodInGCThrashing(now, lastTimeWokeUp);
    periodIsThrashing.offer(wasThrashing);
  }

  /**
   * Dumps the heap to a file and return the name of the file, or
   * <code>null</code> if the heap could not be dumped.
   *
   * @return The name of the file the heap was dumped to.
   */
  private File tryToDumpHeap() {
    // clearing this list should "release" some memory
    // that will be needed to dump the heap
    reservedForDumpingHeap = null;

    try {
      return dumpHeap();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Return a string describing the current memory state of the server.
   */
  public String describeMemory() {
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    long totalMemory = runtime.totalMemory();
    long usedMemory = totalMemory - runtime.freeMemory();
    return String.format(
        "used/total/max = %d/%d/%d MB, GC last/max = %.2f/%.2f %%, #pushbacks=%d, gc thrashing=%s",
        usedMemory >> 20, totalMemory >> 20, maxMemory >> 20, lastMeasuredGCPercentage.get(),
        maxGCPercentage.get(), numPushbacks.get(), isThrashing.get());
  }

  /**
   * Runs this thread.
   */
  @Override
  public void run() {
    checkState(!isStarted.getAndSet(true), "run() called twice");

    try {
      long lastTimeWokeUp = System.currentTimeMillis();
      long lastLog = -1;
      int currentThrashingCount = 0;
      while (true) {
        Thread.sleep(sleepTimeMillis);
        long now = System.currentTimeMillis();

        updateData(now, lastTimeWokeUp);
        updateIsThrashing();

        if (lastLog < 0 || lastLog + NORMAL_LOGGING_PERIOD_MILLIS < now) {
          LOG.info("Memory is {}", describeMemory());
          lastLog = now;
        }

        if (isThrashing.get()) {
          currentThrashingCount++;

          if (shutDownAfterNumGCThrashing > 0
              && (currentThrashingCount >= shutDownAfterNumGCThrashing)) {
            File heapDumpFile = tryToDumpHeap();
            LOG.error(
                "Shutting down JVM after {} consecutive periods of measured GC thrashing. "
                + "Memory is {}. Heap dump written to {}",
                currentThrashingCount, describeMemory(), heapDumpFile);
            System.exit(1);
          }
        } else {
          // Reset the counter whenever the server is evaluated not under gc thrashing.
          currentThrashingCount = 0;
        }

        lastTimeWokeUp = now;
      }
    } catch (InterruptedException e) {
      // most probably means that the server is shutting down
      // in any case, there's not much we can do here
      LOG.info("The GCThrashingMonitor was interrupted.");
    }
  }

  /**
   * Return only when the server is not in the GC thrashing state.
   */
  public void waitForResources(String context) {
    if (!isThrashing.get()) {
      return;
    }
    numPushbacks.incrementAndGet();
    LOG.info("Waiting for resources for {}. Memory is {}", context, describeMemory());
    synchronized (waitingForResources) {
      // No big deal if isThrashing became false in the meantime.
      while (isThrashing.get()) {
        try {
          waitingForResources.wait();
        } catch (InterruptedException e1) {
          LOG.debug("waitForResources was interrupted.");
        }
      }
    }
    LOG.info("Resources granted for {}. Memory is {}", context, describeMemory());
  }

  /**
   * Return the path for logging heap dumps.
   */
  private static String getLoggingDir() {
    String defaultPath = System.getProperty("java.io.tmpdir", DEFAULT_LOGGING_DIR);
    String jsonLogFile = System.getProperty("dataflow.worker.logging.filepath");
    if (jsonLogFile == null) {
      return defaultPath;
    }
    String logPath = new File(jsonLogFile).getParent();
    if (logPath == null) {
      return defaultPath;
    }
    return logPath;
  }

  /**
   * Dump the current heap profile to a file and return its name.
   */
  public static File dumpHeap() throws
      MalformedObjectNameException, InstanceNotFoundException, ReflectionException, MBeanException {
    boolean liveObjectsOnly = true;
    String fileName = String.format(
        "%s/heap_dump_%d.hprof", getLoggingDir(), System.currentTimeMillis());

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName oname = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
    Object[] parameters = {fileName, liveObjectsOnly};
    String[] signatures = {"java.lang.String", boolean.class.getName()};
    mbs.invoke(oname, "dumpHeap", parameters, signatures);

    return new File(fileName);
  }
}
