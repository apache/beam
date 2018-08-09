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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
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
  private static final int TTL_IN_SECONDS = 30;
  private static final int MAX_RETRY = 3;

  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());

  private final ConcurrentHashMap<String, WrappedContext> keyRegistry = new ConcurrentHashMap<>();
  private final ThrowingFunction<JobInfo, FlinkExecutableStageContext> creator;

  public static ReferenceCountingFlinkExecutableStageContextFactory create(
      ThrowingFunction<JobInfo, FlinkExecutableStageContext> creator) {
    return new ReferenceCountingFlinkExecutableStageContextFactory(creator);
  }

  private ReferenceCountingFlinkExecutableStageContextFactory(
      ThrowingFunction<JobInfo, FlinkExecutableStageContext> creator) {
    this.creator = creator;
  }

  @Override
  public FlinkExecutableStageContext get(JobInfo jobInfo) {
    // Retry is needed in case where an existing wrapper is picked from the keyRegistry but by
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
          keyRegistry.computeIfAbsent(
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

  private void scheduleRelease(JobInfo jobInfo) {
    WrappedContext wrapper = keyRegistry.get(jobInfo.jobId());
    Preconditions.checkState(
        wrapper != null, "Releasing context for unknown job: " + jobInfo.jobId());
    // Schedule task to clean the container later.
    @SuppressWarnings("FutureReturnValueIgnored")
    ScheduledFuture unused =
        executor.schedule(() -> release(wrapper), TTL_IN_SECONDS, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  void release(FlinkExecutableStageContext context) {
    @SuppressWarnings({"unchecked", "Not exected to be called from outside."})
    WrappedContext wrapper = (WrappedContext) context;
    synchronized (wrapper) {
      if (wrapper.referenceCount.decrementAndGet() == 0) {
        // Tombstone wrapper.
        wrapper.referenceCount = null;
        if (keyRegistry.remove(wrapper.jobInfo.jobId(), wrapper)) {
          try {
            wrapper.closeActual();
          } catch (Exception e) {
            LOG.error("Unable to close.", e);
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
    public StateRequestHandler getStateRequestHandler(
        ExecutableStage executableStage, RuntimeContext runtimeContext) {
      return context.getStateRequestHandler(executableStage, runtimeContext);
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
}
