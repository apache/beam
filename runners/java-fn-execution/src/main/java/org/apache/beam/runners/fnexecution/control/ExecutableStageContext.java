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
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;

/** The context required in order to execute {@link ExecutableStage stages}. */
public interface ExecutableStageContext extends AutoCloseable {

  /**
   * Creates {@link ExecutableStageContext} instances. Serializable so that factories can be defined
   * at translation time and distributed to TaskManagers.
   */
  interface Factory extends Serializable {

    /** Get or create {@link ExecutableStageContext} for given {@link JobInfo}. */
    ExecutableStageContext get(JobInfo jobInfo);
  }

  StageBundleFactory getStageBundleFactory(ExecutableStage executableStage);
}
