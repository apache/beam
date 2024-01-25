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
package org.apache.beam.runners.jobsubmission;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/** Factory to create {@link JobInvocation} instances. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class JobInvoker {

  private final ListeningExecutorService executorService;

  /** Start running a job, abstracting its state as a {@link JobInvocation} instance. */
  protected abstract JobInvocation invokeWithExecutor(
      RunnerApi.Pipeline pipeline,
      Struct options,
      @Nullable String retrievalToken,
      ListeningExecutorService executorService)
      throws IOException;

  JobInvocation invoke(RunnerApi.Pipeline pipeline, Struct options, @Nullable String retrievalToken)
      throws IOException {
    return invokeWithExecutor(pipeline, options, retrievalToken, this.executorService);
  }

  private ListeningExecutorService createExecutorService(String name) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat(name).setDaemon(true).build();
    return MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(threadFactory));
  }

  protected JobInvoker(String name) {
    this.executorService = createExecutorService(name);
  }
}
