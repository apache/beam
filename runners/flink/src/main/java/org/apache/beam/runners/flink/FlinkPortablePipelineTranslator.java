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
package org.apache.beam.runners.flink;

import java.util.List;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.flink.api.common.JobExecutionResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface for portable Flink translators. This allows for a uniform invocation pattern for
 * pipeline translation between streaming and portable runners.
 *
 * <p>Pipeline translators will generally provide a mechanism to produce the translation contexts
 * that they use for pipeline translation. Post translation, the translation context should contain
 * a pipeline plan that has not yet been executed.
 */
public interface FlinkPortablePipelineTranslator<
    T extends FlinkPortablePipelineTranslator.TranslationContext> {

  /** The context used for pipeline translation. */
  interface TranslationContext {
    JobInfo getJobInfo();

    FlinkPipelineOptions getPipelineOptions();
  }

  /** A handle used to execute a translated pipeline. */
  interface Executor {
    JobExecutionResult execute(String jobName) throws Exception;
  }

  T createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage);

  Set<String> knownUrns();

  /** Translates the given pipeline. */
  Executor translate(T context, RunnerApi.Pipeline pipeline);
}
