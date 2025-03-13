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
package org.apache.beam.runners.dataflow;

import com.google.api.services.dataflow.model.Environment;

/**
 * An instance of this class can be passed to the {@link DataflowRunner} to add user defined hooks
 * to be invoked at various times during pipeline execution.
 */
public class DataflowRunnerHooks {
  /**
   * Allows the user to modify the environment of their job before their job is submitted to the
   * service for execution.
   *
   * @param environment The environment of the job. Users can make change to this instance in order
   *     to change the environment with which their job executes on the service.
   */
  public void modifyEnvironmentBeforeSubmission(Environment environment) {}
}
