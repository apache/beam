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
package org.apache.beam.runners.fnexecution.control;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExecutableStageContext.Factory} which counts ExecutableStageContext reference for book
 * keeping.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ReferenceCountingExecutableStageContextFactory
    implements ExecutableStageContext.Factory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReferenceCountingExecutableStageContextFactory.class);
  private static final int MAX_RETRY = 3;

  private final Creator creator;
  private transient volatile ScheduledExecutorService executor;
  private transient volatile ConcurrentHashMap<String, WrappedContext> keyRegistry;
  private final SerializableFunction<Object, Boolean> isReleaseSynchronous;

  public static ReferenceCountingExecutableStageContextFactory create(
      Creator creator, SerializableFunction<Object, Boolean> isReleaseSynchronous) {
    return new ReferenceCountingExecutableStageContextFactory(creator, isReleaseSynchronous);
  }

  private ReferenceCountingExecutableStageContextFactory(
      Creator creator, SerializableFunction<Object, Boolean> isReleaseSynchronous) {
    this.creator = creator;
    this.isReleaseSynchronous = isReleaseSynchronous;
  }

  @Override
  public ExecutableStageContext get(JobInfo jobInfo) {
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

    PipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    int environmentCacheTTLMillis =
        pipelineOptions.as(PortablePipelineOptions.class).getEnvironmentCacheMillis();
    if (environmentCacheTTLMillis > 0) {
      if (isReleaseSynchronous.apply(this)) {
        // Do immediate cleanup
        release(wrapper);
      } else {
        // Schedule task to clean the container later.
        getExecutor()
            .schedule(() -> release(wrapper), environmentCacheTTLMillis, TimeUnit.MILLISECONDS);
      }
    } else {
      // Do not release this asynchronously, as the releasing could fail due to the classloader not
      // being available anymore after the tasks have been removed from the execution engine.
      release(wrapper);
    }
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
            Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                    .setNameFormat("ScheduledExecutor-thread")
                    .setDaemon(true)
                    .build());
      }
      return executor;
    }
  }

  @VisibleForTesting
  void release(ExecutableStageContext context) {
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
            LOG.error("Unable to close ExecutableStageContext.", t);
          }
        }
      }
    }
  }

  /** {@link WrappedContext} does not expose equals of actual {@link ExecutableStageContext}. */
  @VisibleForTesting
  class WrappedContext implements ExecutableStageContext {
    private JobInfo jobInfo;
    private AtomicInteger referenceCount;
    @VisibleForTesting ExecutableStageContext context;

    /** {@link WrappedContext#equals(Object)} is only based on {@link JobInfo#jobId()}. */
    WrappedContext(JobInfo jobInfo, ExecutableStageContext context) {
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
    public boolean equals(@Nullable Object o) {
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
      extends ThrowingFunction<JobInfo, ExecutableStageContext>, Serializable {}
}
