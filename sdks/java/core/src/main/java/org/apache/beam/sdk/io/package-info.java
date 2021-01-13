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
 * Defines transforms for reading and writing common storage formats, including {@link
 * org.apache.beam.sdk.io.AvroIO}, and {@link org.apache.beam.sdk.io.TextIO}.
 *
 * <p>The classes in this package provide {@code Read} transforms that create PCollections from
 * existing storage:
 *
 * <pre>{@code
 * PCollection<TableRow> inputData = pipeline.apply(
 *     BigQueryIO.readTableRows().from("clouddataflow-readonly:samples.weather_stations"));
 * }</pre>
 *
 * and {@code Write} transforms that persist PCollections to external storage:
 *
 * <pre>{@code
 * PCollection<Integer> numbers = ...;
 * numbers.apply(TextIO.write().to("gs://my_bucket/path/to/numbers"));
 * }</pre>
 */
@DefaultAnnotation(NonNull.class)
package org.apache.beam.sdk.io;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import org.checkerframework.checker.nullness.qual.NonNull;
