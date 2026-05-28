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
 * I/O connector for <a href="https://arrow.apache.org/docs/format/Flight.html">Apache Arrow
 * Flight</a>.
 *
 * <p>Arrow Flight is a high-performance RPC framework for fast data transport using the Apache
 * Arrow columnar format over gRPC. This connector enables Beam pipelines to read from and write to
 * any Arrow Flight-compatible data system, including Dremio, ClickHouse, Apache Doris, InfluxDB 3,
 * DataFusion, and custom Flight servers.
 *
 * @see org.apache.beam.sdk.io.arrowflight.ArrowFlightIO
 */
package org.apache.beam.sdk.io.arrowflight;
