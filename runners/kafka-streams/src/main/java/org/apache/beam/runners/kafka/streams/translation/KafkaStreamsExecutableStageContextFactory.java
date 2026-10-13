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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/**
 * Provides one {@link ExecutableStageContext.Factory} per job for the Kafka Streams runner.
 *
 * <p>Mirrors {@code FlinkExecutableStageContextFactory}: a singleton that hands out reference-
 * counted {@link DefaultExecutableStageContext}s keyed by job id, so the SDK harness environment
 * for a job is created once and shared across the {@link ImpulseProcessor}/executable-stage
 * processors that run within the same JVM instance.
 */
public class KafkaStreamsExecutableStageContextFactory implements ExecutableStageContext.Factory {

  private static final KafkaStreamsExecutableStageContextFactory INSTANCE =
      new KafkaStreamsExecutableStageContextFactory();

  private final ConcurrentMap<String, ExecutableStageContext.Factory> jobFactories =
      new ConcurrentHashMap<>();

  private KafkaStreamsExecutableStageContextFactory() {}

  public static KafkaStreamsExecutableStageContextFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public ExecutableStageContext get(JobInfo jobInfo) {
    ExecutableStageContext.Factory jobFactory =
        jobFactories.computeIfAbsent(
            jobInfo.jobId(),
            k ->
                ReferenceCountingExecutableStageContextFactory.create(
                    DefaultExecutableStageContext::create,
                    // Release the context synchronously once its reference count drops to zero,
                    // and also drop the per-job factory entry so a long-lived JVM that runs many
                    // jobs does not accumulate one entry per finished job.
                    (caller) -> {
                      jobFactories.remove(k);
                      return true;
                    }));
    return jobFactory.get(jobInfo);
  }
}
