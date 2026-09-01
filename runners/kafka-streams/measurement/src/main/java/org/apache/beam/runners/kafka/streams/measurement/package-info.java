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
 * An application for measuring the Kafka Streams runner's behaviour when instances come and go.
 *
 * <p>Not part of the build's verification: it is something a person runs against a Kafka, several
 * copies at once, and watches. See {@link
 * org.apache.beam.runners.kafka.streams.measurement.RescalingMeasurement} for how to run it.
 */
package org.apache.beam.runners.kafka.streams.measurement;
