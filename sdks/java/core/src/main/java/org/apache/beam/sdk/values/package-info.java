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
 * Defines {@link org.apache.beam.sdk.values.PCollection} and other classes for representing data in
 * a {@link org.apache.beam.sdk.Pipeline}.
 *
 * <p>In particular, see these collection abstractions:
 *
 * <ul>
 *   <li>{@link org.apache.beam.sdk.values.PCollection} - an immutable collection of values of type
 *       {@code T} and the main representation for data in Beam.
 *   <li>{@link org.apache.beam.sdk.values.PCollectionView} - an immutable view of a {@link
 *       org.apache.beam.sdk.values.PCollection} that can be accessed as a side input of a {@link
 *       org.apache.beam.sdk.transforms.ParDo} {@link org.apache.beam.sdk.transforms.PTransform}.
 *   <li>{@link org.apache.beam.sdk.values.PCollectionTuple} - a heterogeneous tuple of {@link
 *       org.apache.beam.sdk.values.PCollection PCollections} used in cases where a {@link
 *       org.apache.beam.sdk.transforms.PTransform} takes or returns multiple {@link
 *       org.apache.beam.sdk.values.PCollection PCollections}.
 *   <li>{@link org.apache.beam.sdk.values.PCollectionList} - a homogeneous list of {@link
 *       org.apache.beam.sdk.values.PCollection PCollections} used, for example, as input to {@link
 *       org.apache.beam.sdk.transforms.Flatten}.
 * </ul>
 *
 * <p>And these classes for individual values play particular roles in Beam:
 *
 * <ul>
 *   <li>{@link org.apache.beam.sdk.values.KV} - a key/value pair that is used by keyed transforms,
 *       most notably {@link org.apache.beam.sdk.transforms.GroupByKey}.
 *   <li>{@link org.apache.beam.sdk.values.TimestampedValue} - a timestamp/value pair that is used
 *       for windowing and handling out-of-order data in streaming execution.
 * </ul>
 *
 * <p>For further details, see the documentation for each class in this package.
 */
@DefaultAnnotation(NonNull.class)
package org.apache.beam.sdk.values;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import org.checkerframework.checker.nullness.qual.NonNull;
