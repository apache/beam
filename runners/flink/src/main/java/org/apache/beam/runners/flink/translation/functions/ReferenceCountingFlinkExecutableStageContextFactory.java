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
package org.apache.beam.runners.flink.translation.functions;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FlinkExecutableStageContext.Factory} which counts FlinkExecutableStageContext reference
 * for book keeping.
 */
public class ReferenceCountingFlinkExecutableStageContextFactory
    implements FlinkExecutableStageContext.Factory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReferenceCountingFlinkExecutableStageContextFactory.class);
  private static final int MAX_RETRY = 3;

  private final Creator creator;
  private transient volatile ScheduledExecutorService executor;
  private transient volatile ConcurrentHashMap<String, WrappedContext> keyRegistry;

  public static ReferenceCountingFlinkExecutableStageContextFactory create(Creator creator) {
    return new ReferenceCountingFlinkExecutableStageContextFactory(creator);
  }

  private ReferenceCountingFlinkExecutableStageContextFactory(Creator creator) {
    this.creator = creator;
  }

  @Override
  public FlinkExecutableStageContext get(JobInfo jobInfo) {
    // Retry is needed in case where an existing wrapper is picked from the cache but by
    // the time we accessed wrapper.referenceCount, the wrapper was tombstoned by a pending
    // release task.
    // This race condition is highly unlikely to happen as there is no systematic coding
    // practice which can cause this error because of TTL. However, even in very unlikely case
    // when it happen we have the retry which get a valid context.
    // Note: There is no leak in this logic as the cleanup is only done in release.
    // In case of usage error where release is called before corresponding get finishes,
    // release might throw an error. If release did not throw an error than we can be sure that
    // the state of the system remains valid and appropriate cleanup will be done at TTL.
    for (int retry = 0; retry < MAX_RETRY; retry++) {
      // ConcurrentHashMap will handle the thread safety at the creation time.
      WrappedContext wrapper =
          getCache()
              .computeIfAbsent(
                  jobInfo.jobId(),
                  jobId -> {
                    try {
                      return new WrappedContext(jobInfo, creator.apply(jobInfo));
                    } catch (Exception e) {
                      throw new RuntimeException(
                          "Unable to create context for job " + jobInfo.jobId(), e);
                    }
                  });
      // Take a lock on wrapper before modifying reference count.
      // Use null referenceCount == null as a tombstone for the wrapper.
      synchronized (wrapper) {
        if (wrapper.referenceCount != null) {
          // The wrapper is still valid.
          // Release has not yet got the lock and has not yet removed the wrapper.
          wrapper.referenceCount.incrementAndGet();
          return wrapper;
        }
      }
    }

    throw new RuntimeException(
        String.format(
            "Max retry %s exhausted while creating Context for job %s",
            MAX_RETRY, jobInfo.jobId()));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleRelease(JobInfo jobInfo) {
    WrappedContext wrapper = getCache().get(jobInfo.jobId());
    Preconditions.checkState(
        wrapper != null, "Releasing context for unknown job: " + jobInfo.jobId());
    // Do not release this asynchronously, as the releasing could fail due to the classloader not being
    // available anymore after the tasks have been removed from the execution engine.
    release(wrapper);
  }

  private ConcurrentHashMap<String, WrappedContext> getCache() {
    // Lazily initialize keyRegistry because serialization will set it to null.
    if (keyRegistry != null) {
      return keyRegistry;
    }
    synchronized (this) {
      if (keyRegistry == null) {
        keyRegistry = new ConcurrentHashMap<>();
      }
      return keyRegistry;
    }
  }

  private ScheduledExecutorService getExecutor() {
    // Lazily initialize executor because serialization will set it to null.
    if (executor != null) {
      return executor;
    }
    synchronized (this) {
      if (executor == null) {
        executor =
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
      }
      return executor;
    }
  }

  @VisibleForTesting
  void release(FlinkExecutableStageContext context) {
    @SuppressWarnings({"unchecked", "Not exected to be called from outside."})
    WrappedContext wrapper = (WrappedContext) context;
    synchronized (wrapper) {
      if (wrapper.referenceCount.decrementAndGet() == 0) {
        // Tombstone wrapper.
        wrapper.referenceCount = null;
        if (getCache().remove(wrapper.jobInfo.jobId(), wrapper)) {
          try {
            wrapper.closeActual();
          } catch (Throwable t) {
            LOG.error("Unable to close FlinkExecutableStageContext.", t);
          }
        }
      }
    }
  }

  /**
   * {@link WrappedContext} does not expose equals of actual {@link FlinkExecutableStageContext}.
   */
  private class WrappedContext implements FlinkExecutableStageContext {
    private JobInfo jobInfo;
    private AtomicInteger referenceCount;
    private FlinkExecutableStageContext context;

    /** {@link WrappedContext#equals(Object)} is only based on {@link JobInfo#jobId()}. */
    WrappedContext(JobInfo jobInfo, FlinkExecutableStageContext context) {
      this.jobInfo = jobInfo;
      this.context = context;
      this.referenceCount = new AtomicInteger(0);
    }

    @Override
    public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
      return context.getStageBundleFactory(executableStage);
    }

    @Override
    public void close() {
      // Just schedule the context as we want to reuse it if possible.
      scheduleRelease(jobInfo);
    }

    private void closeActual() throws Exception {
      context.close();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WrappedContext that = (WrappedContext) o;
      return Objects.equals(jobInfo.jobId(), that.jobInfo.jobId());
    }

    @Override
    public int hashCode() {
      return Objects.hash(jobInfo);
    }

    @Override
    public String toString() {
      return "ContextWrapper{"
          + "jobId='"
          + jobInfo
          + '\''
          + ", referenceCount="
          + referenceCount
          + '}';
    }
  }

  /** Interface for creator which extends Serializable. */
  public interface Creator
      extends ThrowingFunction<JobInfo, FlinkExecutableStageContext>, Serializable {}
}
