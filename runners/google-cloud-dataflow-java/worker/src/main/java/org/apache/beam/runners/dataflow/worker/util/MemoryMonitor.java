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
package org.apache.beam.runners.dataflow.worker.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.AtomicDouble;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable which monitors a server for GC thrashing.
 *
 * <p>Note: Only one instance of this should be initialized per server and it should be done when
 * the server starts running.
 *
 * <p>This runnable works as follows:
 *
 * <ul>
 *   <li>It wakes up periodically and determines how much time was spend on garbage collection since
 *       the last time it woke up.
 *   <li>If the time spent in garbage collection in the last period of time exceeds a certain
 *       threshold, that period is marked as "being in GC thrashing"
 *   <li>It keeps track of the GC thrashing status of the last few periods.
 *   <li>Every time the runnable's thread wakes up, it computes the ratio {@code (# monitored
 *       periods in GC thrashing) / (# monitored periods)}.
 *   <li>If this ratio exceeds a certain threshold, it is assumed that the server is in GC
 *       thrashing.
 *   <li>It can also shutdown the current jvm runtime when a threshold of consecutive gc thrashing
 *       count is met. A heap dump is made before shutdown.
 * </ul>
 */
public class MemoryMonitor implements Runnable, StatusDataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitor.class);

  /** Directory to hold heap dumps if not overridden. */
  private static final String DEFAULT_LOGGING_DIR = "dataflow/logs";

  /** Amount of time (in ms) this thread must sleep between two consecutive iterations. */
  public static final long DEFAULT_SLEEP_TIME_MILLIS = 15 * 1000; // 15 sec.

  /**
   * The number of periods to take into account when determining if the server is in GC thrashing.
   */
  private static final int NUM_MONITORED_PERIODS = 4; // ie 1 min's worth.

  /**
   * The <code>(# monitored periods in GC thrashing) / (# monitored
   * periods)</code> threshold after which the server is considered to be in GC thrashing, expressed
   * as a percentage.
   */
  private static final double GC_THRASHING_PERCENTAGE_PER_SERVER = 60.0;

  /**
   * The amount of memory (in bytes) we should pre-allocate, in order to be able to dump the heap.
   *
   * <p>Since the server is in GC thrashing when we try to dump the heap, we might not be able to
   * successfully do it. However, if we pre-allocate a big enough block of memory and "release" it
   * right before trying to dump the heap, the pre-allocated block of memory will get GCed, and the
   * heap dump might succeed.
   */
  private static final int HEAP_DUMP_RESERVED_BYTES = 10 << 20; // 10MB

  /**
   * Shutdown the current JVM instance after given consecutive gc thrashing periods are detected.
   * This offers an opportunity to fast kill a JVM server if it is about to enter a long lasting gc
   * thrashing state, which is almost never a desired behavior for a healthy server. 0 to disable.
   */
  private static final int DEFAULT_SHUT_DOWN_AFTER_NUM_GCTHRASHING = 8; // ie 2 min's worth.

  /** Delay between logging the current memory state. */
  private static final int NORMAL_LOGGING_PERIOD_MILLIS = 5 * 60 * 1000; // 5 min.

  /** Abstract interface for providing GC stats (for testing). */
  public interface GCStatsProvider {
    /** Return the total milliseconds spent in GC since JVM was started. */
    long totalGCTimeMilliseconds();
  }

  /** True system GC stats. */
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
   * The state of the periods that are taken into account when deciding if the server is in GC
   * thrashing.
   */
  private final Queue<Boolean> periodIsThrashing = new ArrayDeque<>();

  /** Keeps track of the time the server spent in GC since it started running. */
  private long timeInGC = 0;

  /**
   * A reserved block of memory, needed to dump the heap. Dumping the heap requires memory. However,
   * since we try to do it when the server is in GC thrashing, no memory is available and dumpHeap()
   * brings the server down. If we pre-allocate a block of memory though, and "release" it right
   * before dumping the heap, this block of memory will be garbage collected, thus giving dumpHeap()
   * enough space to dump the heap.
   */
  @SuppressFBWarnings("unused")
  private byte[] reservedForDumpingHeap = new byte[HEAP_DUMP_RESERVED_BYTES];

  /** If true, dump the heap when thrashing or requested. */
  private final boolean canDumpHeap;

  /**
   * The GC thrashing threshold for every period. If the time spent on garbage collection in one
   * period exceeds this threshold, that period is considered to be in GC thrashing.
   */
  private final double gcThrashingPercentagePerPeriod;

  private final AtomicBoolean isThrashing = new AtomicBoolean(false);

  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private final AtomicDouble lastMeasuredGCPercentage = new AtomicDouble(0.0);
  private final AtomicDouble maxGCPercentage = new AtomicDouble(0.0);
  private final AtomicInteger numPushbacks = new AtomicInteger(0);

  /** Wait point for threads in pushback waiting for gc thrashing to pass. */
  private final Object waitingForResources = new Object();

  /** Wait point for threads wanting to wait for change to isRunning or isThrashing state. */
  private final Object waitingForStateChange = new Object();

  /**
   * If non null, if a heap dump is detected during initialization upload it to the given GCS path.
   */
  private final @Nullable String uploadToGCSPath;

  private final File localDumpFolder;

  public static MemoryMonitor fromOptions(PipelineOptions options) {
    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    String uploadToGCSPath = debugOptions.getSaveHeapDumpsToGcsPath();
    boolean canDumpHeap = uploadToGCSPath != null || debugOptions.getDumpHeapOnOOM();
    double gcThrashingPercentagePerPeriod = debugOptions.getGCThrashingPercentagePerPeriod();

    return new MemoryMonitor(
        new SystemGCStatsProvider(),
        DEFAULT_SLEEP_TIME_MILLIS,
        DEFAULT_SHUT_DOWN_AFTER_NUM_GCTHRASHING,
        canDumpHeap,
        gcThrashingPercentagePerPeriod,
        uploadToGCSPath,
        getLoggingDir());
  }

  @VisibleForTesting
  static MemoryMonitor forTest(
      GCStatsProvider gcStatsProvider,
      long sleepTimeMillis,
      int shutDownAfterNumGCThrashing,
      boolean canDumpHeap,
      double gcThrashingPercentagePerPeriod,
      @Nullable String uploadToGCSPath,
      File localDumpFolder) {
    return new MemoryMonitor(
        gcStatsProvider,
        sleepTimeMillis,
        shutDownAfterNumGCThrashing,
        canDumpHeap,
        gcThrashingPercentagePerPeriod,
        uploadToGCSPath,
        localDumpFolder);
  }

  private MemoryMonitor(
      GCStatsProvider gcStatsProvider,
      long sleepTimeMillis,
      int shutDownAfterNumGCThrashing,
      boolean canDumpHeap,
      double gcThrashingPercentagePerPeriod,
      @Nullable String uploadToGCSPath,
      File localDumpFolder) {
    this.gcStatsProvider = gcStatsProvider;
    this.sleepTimeMillis = sleepTimeMillis;
    this.shutDownAfterNumGCThrashing = shutDownAfterNumGCThrashing;
    this.canDumpHeap = canDumpHeap;
    this.gcThrashingPercentagePerPeriod = gcThrashingPercentagePerPeriod;
    this.uploadToGCSPath = uploadToGCSPath;
    this.localDumpFolder = localDumpFolder;
  }

  /** For testing only: Wait for the monitor to be running. */
  @VisibleForTesting
  void waitForRunning() {
    synchronized (waitingForStateChange) {
      boolean interrupted = false;
      try {
        while (!isRunning.get()) {
          try {
            waitingForStateChange.wait();
          } catch (InterruptedException e) {
            interrupted = true;
            // Retry test.
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** For testing only: Wait for thrashing status to be updated to given value. */
  @VisibleForTesting
  public void waitForThrashingState(boolean desiredThrashingState) {
    synchronized (waitingForStateChange) {
      boolean interrupted = false;
      try {
        while (isThrashing.get() != desiredThrashingState) {
          try {
            waitingForStateChange.wait();
          } catch (InterruptedException e) {
            interrupted = true;
            // Retry test.
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private File getDefaultHeapDumpPath() {
    return new File(localDumpFolder, "heap_dump.hprof");
  }

  @VisibleForTesting
  boolean tryUploadHeapDumpIfItExists() {
    if (uploadToGCSPath == null) {
      return false;
    }

    boolean uploadedHeapDump = false;
    File localSource = getDefaultHeapDumpPath();
    LOG.info("Looking for heap dump at {}", localSource);
    if (localSource.exists()) {
      LOG.warn("Heap dump {} detected, attempting to upload to GCS", localSource);
      String remoteDest =
          String.format("%s/heap_dump%s.hprof", uploadToGCSPath, UUID.randomUUID().toString());
      ResourceId resource = FileSystems.matchNewResource(remoteDest, false);
      try {
        uploadFileToGCS(localSource, resource);
        uploadedHeapDump = true;
        LOG.warn("Heap dump {} uploaded to {}", localSource, remoteDest);
      } catch (IOException e) {
        LOG.error("Error uploading heap dump to {}", remoteDest, e);
      }

      try {
        Files.delete(localSource.toPath());
        LOG.info("Deleted local heap dump {}", localSource);
      } catch (IOException e) {
        LOG.warn("Unable to delete local heap dump {}", localSource, e);
      }
    }

    return uploadedHeapDump;
  }

  private void uploadFileToGCS(File srcPath, ResourceId destination) throws IOException {
    StandardCreateOptions createOptions =
        StandardCreateOptions.builder().setMimeType("application/octet-stream").build();
    try (WritableByteChannel dst = FileSystems.create(destination, createOptions)) {
      try (ReadableByteChannel src = Channels.newChannel(new FileInputStream(srcPath))) {
        ByteStreams.copy(src, dst);
      }
    }
  }

  /** Request the memory monitor stops. */
  public void stop() {
    synchronized (waitingForStateChange) {
      isRunning.set(false);
      waitingForStateChange.notifyAll();
    }
  }

  public boolean isThrashing() {
    return isThrashing.get();
  }

  /**
   * Check if we've observed high gc workload in sufficient sample periods to justify classifying
   * the server as in gc thrashing.
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
    boolean serverInGcThrashing =
        (numPeriodsInGCThrashing * 100
            >= periodIsThrashing.size() * GC_THRASHING_PERCENTAGE_PER_SERVER);
    setIsThrashing(serverInGcThrashing);
  }

  /** Set the thrashing state. */
  private void setIsThrashing(boolean serverInGcThrashing) {
    synchronized (waitingForResources) {
      synchronized (waitingForStateChange) {
        boolean prev = isThrashing.getAndSet(serverInGcThrashing);
        if (prev && !serverInGcThrashing) {
          waitingForResources.notifyAll();
        }
        if (prev != serverInGcThrashing) {
          waitingForStateChange.notifyAll();
        }
      }
    }
  }

  /**
   * Determines if too much time was spent on garbage collection in the last period of time.
   *
   * @param now The current time.
   * @param lastTimeWokeUp The last time this thread woke up.
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
    double gcPercentage = (inGC - timeInGC) * 100.0 / (now - lastTimeWokeUp);

    lastMeasuredGCPercentage.set(gcPercentage);
    maxGCPercentage.set(Math.max(maxGCPercentage.get(), gcPercentage));
    timeInGC = inGC;

    return gcPercentage > this.gcThrashingPercentagePerPeriod;
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
   * Dumps the heap to a file and return the name of the file, or {@literal null} if the heap should
   * not or could not be dumped.
   *
   * @return The name of the file the heap was dumped to, otherwise {@literal null}.
   */
  public @Nullable File tryToDumpHeap() {
    if (!canDumpHeap) {
      return null;
    }

    // Clearing this list should "release" some memory that will be needed to dump the heap.
    // We could try to reallocate it again if we later notice memory pressure has subsided,
    // but that is risk. Further, leaving this released may help with the memory pressure.
    reservedForDumpingHeap = null;

    try {
      return dumpHeap();
    } catch (Exception e) {
      LOG.warn("Unable to dump heap: ", e);
      return null;
    }
  }

  @SuppressFBWarnings("DM_EXIT") // we deliberately System.exit under memory
  private void shutDownDueToGcThrashing(int thrashingCount) {
    File heapDumpFile = tryToDumpHeap();
    LOG.error(
        "Shutting down JVM after {} consecutive periods of measured GC thrashing. "
            + "Memory is {}. Heap dump {}.",
        thrashingCount,
        describeMemory(),
        heapDumpFile == null ? "not written" : ("written to '" + heapDumpFile + "'"));

    System.exit(1);
  }

  /** Runs this thread. */
  @Override
  public void run() {
    synchronized (waitingForStateChange) {
      Preconditions.checkState(!isRunning.getAndSet(true), "already running");

      if (this.gcThrashingPercentagePerPeriod <= 0 || this.gcThrashingPercentagePerPeriod >= 100) {
        LOG.warn(
            "gcThrashingPercentagePerPeriod: {} is not valid value. Not starting MemoryMonitor.",
            this.gcThrashingPercentagePerPeriod);
        isRunning.set(false);
      }

      waitingForStateChange.notifyAll();
    }

    // Within the memory monitor thread check to see if there is a pre-existing heap dump, and
    // attempt to upload it. Note that this will delay the first memory monitor check.
    tryUploadHeapDumpIfItExists();

    try {
      long lastTimeWokeUp = System.currentTimeMillis();
      long lastLog = -1;
      int currentThrashingCount = 0;
      while (true) {
        synchronized (waitingForStateChange) {
          waitingForStateChange.wait(sleepTimeMillis);
        }
        if (!isRunning.get()) {
          break;
        }
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
            shutDownDueToGcThrashing(currentThrashingCount);
          }
        } else {
          // Reset the counter whenever the server is evaluated not under gc thrashing.
          currentThrashingCount = 0;
        }

        lastTimeWokeUp = now;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // most probably means that the server is shutting down
      // in any case, there's not much we can do here
      LOG.info("The GCThrashingMonitor was interrupted.");
    }
  }

  /** Return only when the server is not in the GC thrashing state. */
  public void waitForResources(String context) {
    if (!isThrashing.get()) {
      return;
    }
    numPushbacks.incrementAndGet();
    LOG.info("Waiting for resources for {}. Memory is {}", context, describeMemory());
    synchronized (waitingForResources) {
      boolean interrupted = false;
      try {
        // No big deal if isThrashing became false in the meantime.
        while (isThrashing.get()) {
          try {
            waitingForResources.wait();
          } catch (InterruptedException e1) {
            interrupted = true;
            LOG.debug("waitForResources was interrupted.");
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    LOG.info("Resources granted for {}. Memory is {}", context, describeMemory());
  }

  /** Return the path for logging heap dumps. */
  private static File getLoggingDir() {
    File defaultPath = new File(System.getProperty("java.io.tmpdir", DEFAULT_LOGGING_DIR));
    String jsonLogFile = System.getProperty("dataflow.worker.logging.filepath");
    if (jsonLogFile == null) {
      return defaultPath;
    }
    File logPath = new File(jsonLogFile).getParentFile();
    if (logPath == null) {
      return defaultPath;
    }
    return logPath;
  }

  /**
   * Dump the current heap profile to a file in the given directory and return its name.
   *
   * <p>NOTE: We deliberately don't salt the heap dump filename so as to minimize disk impact of
   * repeated dumps. These files can be of comparable size to the local disk.
   */
  public File dumpHeap()
      throws MalformedObjectNameException, InstanceNotFoundException, ReflectionException,
          MBeanException, IOException {
    return dumpHeap(localDumpFolder);
  }

  /**
   * Dump the current heap profile to a file in the given directory and return its name.
   *
   * <p>NOTE: We deliberately don't salt the heap dump filename so as to minimize disk impact of
   * repeated dumps. These files can be of comparable size to the local disk.
   */
  @VisibleForTesting
  static synchronized File dumpHeap(File directory)
      throws MalformedObjectNameException, InstanceNotFoundException, ReflectionException,
          MBeanException, IOException {
    boolean liveObjectsOnly = false;
    File fileName = new File(directory, "heap_dump.hprof");
    if (fileName.exists() && !fileName.delete()) {
      throw new IOException("heap_dump.hprof already existed and couldn't be deleted!");
    }

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName oname = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
    Object[] parameters = {fileName.getPath(), liveObjectsOnly};
    String[] signatures = {String.class.getName(), boolean.class.getName()};
    mbs.invoke(oname, "dumpHeap", parameters, signatures);

    Files.setPosixFilePermissions(
        fileName.toPath(),
        ImmutableSet.of(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ));

    LOG.warn("Heap dumped to {}", fileName);

    return fileName;
  }

  /** Return a string describing the current memory state of the server. */
  private String describeMemory() {
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    long totalMemory = runtime.totalMemory();
    long usedMemory = totalMemory - runtime.freeMemory();
    return String.format(
        "used/total/max = %d/%d/%d MB, GC last/max = %.2f/%.2f %%, #pushbacks=%d, gc thrashing=%s",
        usedMemory >> 20,
        totalMemory >> 20,
        maxMemory >> 20,
        lastMeasuredGCPercentage.get(),
        maxGCPercentage.get(),
        numPushbacks.get(),
        isThrashing.get());
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.print("Memory: ");
    writer.print(describeMemory());
    writer.println("<br>");
  }
}
