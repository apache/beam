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
 * Defines {@link org.apache.beam.sdk.coders.Coder Coders} to specify how data is encoded to and
 * decoded from byte strings.
 *
 * <p>During execution of a Pipeline, elements in a {@link org.apache.beam.sdk.values.PCollection}
 * may need to be encoded into byte strings. This happens both at the beginning and end of a
 * pipeline when data is read from and written to persistent storage and also during execution of a
 * pipeline when elements are communicated between machines.
 *
 * <p>Exactly when PCollection elements are encoded during execution depends on which {@link
 * org.apache.beam.sdk.PipelineRunner} is being used and how that runner chooses to execute the
 * pipeline. As such, Beam requires that all PCollections have an appropriate Coder in case it
 * becomes necessary. In many cases, the Coder can be inferred from the available Java type
 * information and the Pipeline's {@link org.apache.beam.sdk.coders.CoderRegistry}. It can be
 * specified per PCollection via {@link org.apache.beam.sdk.values.PCollection#setCoder(Coder)} or
 * per type using the {@link org.apache.beam.sdk.coders.DefaultCoder} annotation.
 *
 * <p>This package provides a number of coders for common types like {@code Integer}, {@code
 * String}, and {@code List}, as well as coders like {@link org.apache.beam.sdk.coders.AvroCoder}
 * that can be used to encode many custom types.
 */
@DefaultAnnotation(NonNull.class)
package org.apache.beam.sdk.coders;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import org.checkerframework.checker.nullness.qual.NonNull;
