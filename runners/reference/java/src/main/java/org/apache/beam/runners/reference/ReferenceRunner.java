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

package org.apache.beam.runners.reference;

import com.google.protobuf.Struct;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;

/**
 * Created by tgroh on 11/8/17.
 */
public class ReferenceRunner {
  public static ReferenceRunnerJob run(Pipeline p, Struct options) {
    /*
    docker run -v WORKER_PERSIST_DIR:SEMI_PERSIST_DIR
      <sdk-harness-container-image> \
      --id=ID \
      --logging_endpoint=LOGGING_ENDPOINT \
      --artifact_endpoint=ARTIFACT_ENDPOINT \
      --provision_endpoint=PROVISION_ENDPOINT \
      --control_endpoint=CONTROL_ENDPOINT \
      --semi_persist_dir=SEMI_PERSIST_DIR
     */
    return new ReferenceRunnerJob();
  }

  public static class ReferenceRunnerJob {}
}
