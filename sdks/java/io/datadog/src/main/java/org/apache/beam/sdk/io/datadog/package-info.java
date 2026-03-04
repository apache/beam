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
 * Transforms for writing to <a href="https://www.datadoghq.com/">Datadog</a>.
 *
 * <p>The {@link org.apache.beam.sdk.io.datadog.DatadogIO} class provides a {@link
 * org.apache.beam.sdk.transforms.PTransform} that allows writing data to the Datadog Logs API.
 *
 * <p>For more information on the Datadog Logs API, see the <a
 * href="https://docs.datadoghq.com/api/latest/logs/">official documentation</a>.
 */
package org.apache.beam.sdk.io.datadog;
