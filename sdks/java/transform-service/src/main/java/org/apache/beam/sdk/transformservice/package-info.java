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
 * A service that can be used to discover and expand transforms.
 *
 * <p>The transform service can be used to discover portable transforms available in Apache Beam and
 * to use such transforms in Apache Beam pipelines. Portable transforms are any transforms that are
 * offered through an expansion service. Discovery functionality can be used to discover
 * Schema-aware transforms availabe through the transform service.
 *
 * <p>The transform service implements the <a
 * href="https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_expansion_api.proto">Beam
 * Expansion API</a>.
 *
 * <p>For more details regarding the design, please see the <a
 * href="https://s.apache.org/beam-transform-service">corresponding design doc</a>.
 */
package org.apache.beam.sdk.transformservice;
