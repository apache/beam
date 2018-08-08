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
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of a {@link FlinkExecutableStageContext} for batch jobs. */
class FlinkBatchExecutableStageContext implements FlinkExecutableStageContext, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBatchExecutableStageContext.class);

  private final JobBundleFactory jobBundleFactory;

  private static FlinkBatchExecutableStageContext create(JobInfo jobInfo) throws Exception {
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.create(jobInfo);
    return new FlinkBatchExecutableStageContext(jobBundleFactory);
  }

  private FlinkBatchExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  @Override
  public StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage, RuntimeContext runtimeContext) {
    SideInputHandlerFactory sideInputHandlerFactory =
        FlinkBatchSideInputHandlerFactory.forStage(executableStage, runtimeContext);
    try {
      return StateRequestHandlers.forSideInputHandlerFactory(
          ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    jobBundleFactory.close();
  }

  enum BatchFactory implements Factory {
    REFERENCE_COUNTING {
      private static final int TTL_IN_SECONDS = 30;
      int MAX_RETRY = 3;

      @SuppressWarnings("Immutable") // observably immutable
      private final ScheduledExecutorService executor =
          Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());

      @SuppressWarnings("Immutable") // observably immutable
      private final ConcurrentHashMap<String, ContextWrapper> keyRegistry =
          new ConcurrentHashMap<>();

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
          ContextWrapper wrapper =
              keyRegistry.computeIfAbsent(
                  jobInfo.jobId(),
                  jobId -> {
                    try {
                      return new ContextWrapper(jobInfo.jobId(), create(jobInfo));
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
              return wrapper.context;
            }
          }
        }

        throw new RuntimeException(
            String.format(
                "Max retry %s exhausted while creating Context for job %s",
                MAX_RETRY, jobInfo.jobId()));
      }

      @Override
      public void release(JobInfo jobInfo) {
        ContextWrapper wrapper = keyRegistry.get(jobInfo.jobId());
        Preconditions.checkState(
            wrapper != null, "Releasing context for unknown job: " + jobInfo.jobId());
        // Schedule task to clean the container later.
        @SuppressWarnings("FutureReturnValueIgnored")
        ScheduledFuture unused =
            executor.schedule(
                () -> {
                  synchronized (wrapper) {
                    if (wrapper.referenceCount.decrementAndGet() == 0) {
                      // Tombstone wrapper.
                      wrapper.referenceCount = null;
                      if (keyRegistry.remove(wrapper.jobId, wrapper)) {
                        try (AutoCloseable context = wrapper.context) {
                        } catch (Exception e) {
                          LOG.error("Unable to close.", e);
                        }
                      }
                    }
                  }
                },
                TTL_IN_SECONDS,
                TimeUnit.SECONDS);
      }

      class ContextWrapper {
        private String jobId;
        private AtomicInteger referenceCount;
        private FlinkBatchExecutableStageContext context;

        ContextWrapper(String jobId, FlinkBatchExecutableStageContext context) {
          this.jobId = jobId;
          this.context = context;
          this.referenceCount = new AtomicInteger(0);
        }

        @Override
        public boolean equals(Object o) {
          if (this == o) {
            return true;
          }
          if (o == null || getClass() != o.getClass()) {
            return false;
          }
          ContextWrapper that = (ContextWrapper) o;
          return Objects.equals(jobId, that.jobId);
        }

        @Override
        public int hashCode() {
          return Objects.hash(jobId);
        }

        @Override
        public String toString() {
          return "ContextWrapper{"
              + "jobId='"
              + jobId
              + '\''
              + ", referenceCount="
              + referenceCount
              + '}';
        }
      }
    }
  }
}
