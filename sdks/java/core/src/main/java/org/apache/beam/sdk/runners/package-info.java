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
 * {@link org.apache.beam.sdk.runners.DirectRunner} and
 * {@link org.apache.beam.sdk.runners.DataflowRunner}.
 *
 * <p>{@link org.apache.beam.sdk.runners.DirectRunner} executes a {@code Pipeline}
 * locally, without contacting the Dataflow service.
 * {@link org.apache.beam.sdk.runners.DataflowRunner} submits a
 * {@code Pipeline} to the Dataflow service, which executes it on Dataflow-managed Compute Engine
 * instances. {@code DataflowRunner} returns
 * as soon as the {@code Pipeline} has been submitted. Use
 * {@link org.apache.beam.sdk.runners.BlockingDataflowRunner} to have execution
 * updates printed to the console.
 *
 * <p>The runner is specified as part {@link org.apache.beam.sdk.options.PipelineOptions}.
 */
package org.apache.beam.sdk.runners;
