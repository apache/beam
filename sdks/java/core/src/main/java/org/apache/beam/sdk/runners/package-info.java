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
/**
 * Defines runners for executing Pipelines in different modes, including
 * {@link com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner} and
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner}.
 *
 * <p>{@link com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner} executes a {@code Pipeline}
 * locally, without contacting the Dataflow service.
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner} submits a
 * {@code Pipeline} to the Dataflow service, which executes it on Dataflow-managed Compute Engine
 * instances. {@code DataflowPipelineRunner} returns
 * as soon as the {@code Pipeline} has been submitted. Use
 * {@link com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner} to have execution
 * updates printed to the console.
 *
 * <p>The runner is specified as part {@link com.google.cloud.dataflow.sdk.options.PipelineOptions}.
 */
package com.google.cloud.dataflow.sdk.runners;
